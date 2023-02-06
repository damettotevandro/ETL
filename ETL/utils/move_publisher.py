import requests 
import zipfile
import io
import sys 
import os 
import glob
import time

pattern = ''

if len(sys.argv) == 3:
    store = str(sys.argv[1])
    path = str(sys.argv[2])
elif len(sys.argv) == 4:
    store = str(sys.argv[1])
    path = str(sys.argv[2])
    pattern = str(sys.argv[3]) # EX.: '*_*.csv'

else:
    raise Exception ('Invalid args...')
        

# Server URL
url = 'https://publisher.nappsolutions.com/download'

# Headers
headers = dict()
headers['content-type'] = 'application/json'

data = '{"username" : "admin","password" : "Napp2599","store" : "'+ store +'", "path" : "' + path + '", "pattern" : "' + pattern + '"}'


print('Download files from: {}'.format(path))
r = requests.get(url, headers=headers, data=data)

# Create zip file
try:
    print('Downloading files...')
    file = zipfile.ZipFile(io.BytesIO(r.content))
except:
    raise Exception ('Creating zip file...ERROR')

time.sleep(5)

# Extract files
try:
    file.extractall('./files')
    zip_files = glob.glob('./files/*.zip')
    for zFile in zip_files:
        zip = zipfile.ZipFile(zFile)
        zip.extractall('./files')
        os.remove(zFile)
    for file_csv in glob.glob('./files/*.csv'):
        os.remove(file_csv)

except:
    raise Exception ('Extracting files...ERROR')

# Finish
print('All files has been moved.')
