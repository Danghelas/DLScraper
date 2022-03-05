import asyncio
import time
import aiohttp
import bs4
import configparser
import json
import re
import requests
import os
import pandas as pd
from datetime import datetime
from os import listdir
from os.path import isfile, join, basename

# User agent to be used for web scraping
headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
requestSession = requests.Session()
requestSession.headers = headers

def main():
    # Load config file
    configInfo = __LoadConfig('../config.ini')

    # Read library directories json file
    pathList = __ReadLibraryDirectories('../LibraryDirectories.json')

    # Scan folders for DL works
    dfFolderInfo = pd.DataFrame(__FolderScan(pathList))
    cleanDLCodes = dfFolderInfo['DLCode'].tolist()
    
    # Import existing json files if exist
    libraryFiles = {
        "age", 
        "author", 
        "circle", 
        "creationDate", 
        "directory",
        "illustration",
        "productFormat",
        "releaseDate",
        "scenario",
        #"summary",
        "series",
        "tag",
        "title",
        "translated",
        "voiceActor"
    }
    
    existingLibrary = {}
    for fileName in libraryFiles:
        file = fileName + ".json"
        existingLibrary[fileName] = __ReadExistingLibraryFile(configInfo['library_indexed_info_path'], file, fileName)

    libraryValid = True
    for key, dataFrame in existingLibrary.items():
        if dataFrame.empty:
            libraryValid = False
    
    if libraryValid:
        # Scrape only newly found works and detect deleted works

        # Detect if there are deleted works and delete them if found
        existingLibrary = __CheckDeletedWorks(existingLibrary, cleanDLCodes)

    else:
        # Library is invalid (file missing) -> Scrape all found works
        
        # Create library structure
        existingLibrary = __GenerateLibraryStructure(libraryFiles)
    
    # Make scrape list
    scrapeList = __GenerateScrapeList(existingLibrary, cleanDLCodes)

    # Progressbar necessities
    totalTasks = len(scrapeList)
    print("Total tasks:" + str(totalTasks*2))

    # Check current downloaded images
    worksWithImages = __ScanForImages(configInfo)

    # Scrape the missing works
    if totalTasks > 0:
        # Scrape info of new works from website, also download their cover images
        newWorkInfo = Scrape(scrapeList, worksWithImages, configInfo)
        #print(newWorkInfo)

        # Add new works to existing library
        newLibrary = __AddWorksToLibrary(existingLibrary, newWorkInfo, dfFolderInfo)
        #print(newLibrary)
    else:
        print('no new products found')

    

    # Save the newly created library to files
    __SaveLibraryToFiles(newLibrary, configInfo)

    print("Sleeping to allow progress bar to catch up")
    time.sleep(1)
    print("Work finished")

def __SaveLibraryToFiles(library: dict, configInfo: dict):
    library_indexed_info_path = configInfo['library_indexed_info_path']
    for key, dfWorkProperty in library.items():
        dfWorkProperty.to_json(library_indexed_info_path + key + '.json', orient='records')

def __AddWorksToLibrary(library: dict, newWorkInfo: dict, dfFolderInfo: pd.DataFrame):
    for work in newWorkInfo:
        # Add each property of a work to the library (age, circle etc.)
        for key, workProperty in work.items(): 
            # Special handling for work properties stored in a list (e.g. tag)
            if isinstance(workProperty, list):
                for value in workProperty:
                    library[key] = library[key].append(
                        pd.DataFrame([[work['DLCode'], value]], columns=['DLCode', key]), ignore_index=True)
            elif key != 'DLCode':
                library[key] = library[key].append(
                        pd.DataFrame([[work['DLCode'], workProperty]], columns=['DLCode', key]), ignore_index=True)
        # Add creationDate & directory
        creationDate = dfFolderInfo.loc[(dfFolderInfo['DLCode'] == work['DLCode'])]['creationDate'].values[0]
        library['creationDate'] = library['creationDate'].append(
            pd.DataFrame([[work['DLCode'], creationDate]], columns=['DLCode', 'creationDate']), ignore_index=True)

        directory = dfFolderInfo.loc[(dfFolderInfo['DLCode'] == work['DLCode'])]['directory'].values[0]
        library['directory'] = library['directory'].append(
            pd.DataFrame([[work['DLCode'], directory]], columns=['DLCode', 'directory']), ignore_index=True)

    return library            

def __ScanForImages(configInfo: dict):
    worksWithImages = [f for f in listdir(configInfo['images_path']) if isfile(join(configInfo['images_path'], f))]
    worksWithImages[:] = [f.split(".", 1)[0] for f in worksWithImages]
    print(worksWithImages)
    return worksWithImages

def __GenerateScrapeList(library: dict, cleanDLCodes: list):
    libraryDLCodes = library['title']['DLCode'].tolist()
    scrapeList = []
    for DLCode in cleanDLCodes:
        if DLCode not in libraryDLCodes:
            scrapeList.append(DLCode)
    return scrapeList

def __GenerateLibraryStructure(libraryFiles: set):
    library = {}
    for fileName in libraryFiles:
        library[fileName] = pd.DataFrame(columns =['DLCode', fileName])
    return library

def __CheckDeletedWorks(library: dict, cleanDLCodes: list):
    dfTitleIter = library['title']
    for row in dfTitleIter.itertuples():
        if row.DLCode not in cleanDLCodes:
            print("Deleted work detected: " + row.DLCode)
            # Delete all records regarding this work
            library = __DeleteFromLibrary(library, row.DLCode)
    
    return library

def __DeleteFromLibrary(library: dict, DLCode: str):
    # Delete all data of work matched by DLCode in all library dataframes
    libraryNew = {}
    for dataFrame in library:
        index_names = dataFrame[ dataFrame['DLCode'] == DLCode].index
        libraryNew[list(dataFrame.columns)[1]] = dataFrame.drop(index_names)
    return libraryNew

def __LoadConfig(configFilePath: str):
    config = configparser.ConfigParser()
    config.read(configFilePath)
    do_translate = config['DEFAULT']['do_translate']
    images_path = "../" + config['DEFAULT']['images_path'] + "/"
    library_indexed_info_path = "../" + config['DEFAULT']['library_indexed_info_path'] + "/"

    return {
        'do_translate': do_translate,
        'images_path': images_path,
        'library_indexed_info_path': library_indexed_info_path
    }

def __ReadLibraryDirectories(filePath: str):
    # Read file which contains all the different paths that should be scanned
    inputFile = open (filePath)
    jsonArray = json.load(inputFile)
    pathList = []
    for path in jsonArray:
        pathList.append(path)
    return pathList

def __FolderScan(pathList: list):
    # Scan listed folders for folders containing a DL work identifier
    # Return a dictionary containing info of each found work:
    # ID (DLCode), directory path, creation date
    dirList = []
    creationDates = []
    cleanDLCodes = []

    for path in pathList:
        path = os.path.normpath(path)
        # Get directory names of DLCodes
        for root,dirs,files in os.walk(path, topdown=True):
            depth = root[len(path) + len(os.path.sep):].count(os.path.sep)
            if depth == 0:
                # We're currently two directories in, so all subdirs have depth 3

                # Detect if foldername contains a DLSite format
                # Following are valid: [RJ123456], [VJ123456]
                for d in dirs:
                    result = re.search(r"\[((RJ|VJ)([0-9]+))\]", d)
                    if result:
                        DLCodeFolderPath = os.path.normpath(os.path.join(root, d))
                        dirList.append(os.path.join(root, d))
                        creationDate = datetime.fromtimestamp(os.stat(DLCodeFolderPath).st_mtime)

                        creationDates += [creationDate.strftime("%Y-%m-%d %H:%M:%S")]

                        cleanDLCodes.append(result.group(1))
                dirs[:] = [] # Don't recurse any deeper
    
    return {
        'DLCode': cleanDLCodes,
        'directory': dirList,
        'creationDate': creationDates
    }

def __ReadExistingLibraryFile(library_indexed_info_path: str, file: str, fileName: str):
    if os.path.exists(library_indexed_info_path + file):
        return pd.read_json(library_indexed_info_path + file)
    else:
        return pd.DataFrame(columns =['DLCode', fileName])

def Scrape(DLCodes: list, worksWithImages, configInfo):
    if not __CheckInput(DLCodes):
        urls = []
        for DLCode in DLCodes:
            urls.append(__GenerateURL(DLCode))
        dfProductInfo = asyncio.get_event_loop().run_until_complete(__ScrapeMain(urls, DLCodes, worksWithImages, configInfo))
        return dfProductInfo
    else:
        print("Input not valid")
        

async def __ScrapeMain(urls, DLCodes, worksWithImages, configInfo):
    async with aiohttp.ClientSession() as session:
        # ret contains list of dictionaries, each dictionary containing the info of one product
        # products without info are registered as a string in the list as "404"
        ret = await asyncio.gather(*[__get(url, DLCodes[count], session, worksWithImages, configInfo) for count, url in enumerate(urls)])
        
        productsFiltered = [y for y in ret if y != "404"]
        return productsFiltered

        #dfProductInfo = pd.concat(productsFiltered)
        #return dfProductInfo

def __convertToSoup(htmlBytes: bytes):
    soup = bs4.BeautifulSoup(htmlBytes, "html.parser")
    return soup

async def __get(url, DLCode,  session: aiohttp.ClientSession, worksWithImages: list, configInfo):
    # Also download image of the work if it's missing
    try:
        async with session.get(url=url) as response:
            if response.status == 200:
                soup = __convertToSoup(await response.read())
                productAttributes = __getProductAttributes(soup, DLCode)
                
                # Check if image needs to be downloaded, or if it's already present in files
                if DLCode not in worksWithImages:
                    __DownloadImage(DLCode, soup, configInfo['images_path'])
                print("Task done")
                return productAttributes
                #return __convertToSoup(await response.read())

            else:
                print("Unable to get url: " + url)
                print("Task done")
                return "404"
    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))
        print("Task done")

def __DownloadImage(DLCode, soupWebpage, imagesPath):
        
    img = soupWebpage.find("li", {"class": "slider_item active"}).find("img")   
    src = img["srcset"]   
    lnk = "http:" + src
    
    extension = basename(lnk).split(".", 1)[1]
    filename = DLCode + "." + extension
    
    saveLocation = imagesPath + '/'+ filename
    
    with open(saveLocation, "wb") as f:
        f.write(requests.get(lnk).content)

def __getProductAttributes(soup, DLCode):
    dfWorkInfo = __getProductMainInfo(soup)
    title = __getTitle(soup)
    tags = __getTags(soup)
    # for tag in tags:
    
    circle = __getCircle(soup)

    age = __getAge(dfWorkInfo)
    author = __getAuthor(dfWorkInfo)
    # for auth in author:
    
    illustration = __getIllustration(dfWorkInfo)
    # for illu in illustration:
    productFormat = __getProductFormat(dfWorkInfo)
    # for format in productFormat:
    releaseDate = __getReleaseDate(dfWorkInfo)
    scenario = __getScenario(dfWorkInfo)
    # for scen in scenario:
    series = __getSeries(dfWorkInfo)
    voiceActors = __getVoiceActors(dfWorkInfo)
    # for voiceActor in voiceActors:

    dfProduct = {
        'DLCode': DLCode,
        'age': age,
        'author': author,
        'circle': circle,
        'illustration': illustration,
        'productFormat': productFormat,
        'releaseDate': releaseDate,
        'scenario': scenario,
        'series': series,
        'tag': tags,
        'title': title,
        'voiceActor': voiceActors
    }
    return dfProduct
    
# ## Check input
def __CheckInput(DLCodes: list):
    valid = True
    
    # Check length of list
    if not __CheckInputLength(DLCodes):
        valid = False
    
    # Check validity of DLCodes
    for DLCode in DLCodes:
        if not __CheckInputDLCode(DLCode):
            valid = False
            break

    return valid
    
def __CheckInputLength(DLCodes: list):
    if len(DLCodes) > 0:
        return True
    else:
        return False

def __CheckInputDLCode(DLCode: str):
    if re.search(r"\[((RJ|VJ)([0-9]+))\]", DLCode):
        return True
    else:
        return False

# ## Generate URL of product from product id
def __GenerateURL(DLCode):
    if "RJ" in DLCode:
        url = "https://www.dlsite.com/maniax/work/=/product_id/" + DLCode + ".html/?locale=en_US"
    elif "VJ" in DLCode:
        url = "https://www.dlsite.com/soft/work/=/product_id/" + DLCode + ".html/?locale=en_US"
    
    return url


# ## Scrape functions
def __getAuthor(df):
    author = df.loc[(df['Header'] == 'Author')]['Data'].values
    if author.size > 0:
        if("/" in author[0]):
            author = author[0].split(" / ")

        author = [auth.strip() for auth in author]
    else:
        author = ['None']
    
    return author

def __getAge(df):
    age = df.loc[(df['Header'] == 'Age')]['Data'].values
    age = __cleanEscapeChars(str(age[0]))
    
    return age

def __getCircle(webpage):
    content = webpage.find("span", {"class": "maker_name"})
    circle = str(content.get_text())
    circle = __cleanEscapeChars(circle)
    
    return circle

def __getIllustration(df):
    illustration = df.loc[(df['Header'] == 'Illustration')]['Data'].values
    if illustration.size > 0:
        if("/" in illustration[0]):
            illustration = illustration[0].split(" / ")

        illustration = [illu.strip() for illu in illustration]
    else:
        illustration = ['None']
    
    return illustration

def __getProductFormat(df):
    productFormat = df.loc[(df['Header'] == 'Product format')]['Data'].values
    productFormat = __cleanEscapeChars(productFormat[0]).strip()
    if "Voice / ASMR" in productFormat:
        productFormat = productFormat.replace("Voice / ASMR", "Voice/ASMR")
    productFormat = productFormat.split(" ")

    return productFormat

def __getReleaseDate(df):
    releaseDate = df.loc[(df['Header'] == 'Release date')]['Data'].values
    releaseDate = __cleanEscapeChars(str(releaseDate[0]))
    
    removeTrailList = list(range(0, 11))
    for trail in removeTrailList:
        removeStr = " " + str(trail)
        releaseDate = releaseDate.replace(removeStr, "")
    
    return releaseDate

def __getScenario(df):
    scenario = df.loc[(df['Header'] == 'Scenario')]['Data'].values
    if scenario.size > 0:
        if("/" in scenario[0]):
            scenario = scenario[0].split(" / ")

        scenario = [scen.strip() for scen in scenario]
    else:
        scenario = ['None']
    
    return scenario

def __getSeries(df):
    series = df.loc[(df['Header'] == 'Series name')]['Data'].values
    if series.size > 0:
        series = __cleanEscapeChars(str(series[0]))
    else:
        series = 'None'
    
    return series

def __getSummary(df):
    # TODO
    return 'todo'

def __getTags(webpage):
    content = webpage.find("div", {"class": "main_genre"})
    content = str(content.get_text())
    tags = content.splitlines()
    tags.pop(0)
    
    return tags

def __getTitle(webpage):
    content = webpage.find("h1", {"id": "work_name"})
    title = str(content.get_text())
    
    title = __cleanEscapeChars(title)
    
    return title

def __getVoiceActors(df):
    voiceActors = df.loc[(df['Header'] == 'Voice Actor')]['Data'].values
    if voiceActors.size > 0:
        if("/" in voiceActors[0]):
            voiceActors = voiceActors[0].split(" / ")

        voiceActors = [VA.strip() for VA in voiceActors]
    else:
        voiceActors = ['None']
    
    return voiceActors

def __getProductMainInfo(webpage):
    data = []
    df = pd.DataFrame(columns =['Header', 'Data'])
    table = webpage.find('table', {'id': 'work_outline'})
    rows = table.find_all('tr')
    for row in rows:
        header = row.find('th')
        data = row.find('td')
        header = str(header.get_text())
        if header == "Product format":
            data = str(data.get_text(separator=' '))
        else:
            data = str(data.get_text())
        
        pairing = [[header, data]]
        dfPairing = pd.DataFrame(pairing, columns=['Header', 'Data'])
        df = df.append(dfPairing, ignore_index=True)
    
    # Get product formats separately due to them all being stringed together by the method above
    
    
    return df

def __cleanEscapeChars(string):
    return str(re.sub('(\n|\r)', '', string))


if __name__ ==  "__main__":
    main()