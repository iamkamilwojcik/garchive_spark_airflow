import subprocess
import os
import shutil
from zipfile import ZipFile

# create requirements file
if os.path.exists("pipfile.lock") == True:
    # create requirements file
    requirements = open("requirements.txt", 'w')
    subprocess.call(["pipenv", "lock", "-r"], stdout= requirements)
    print("requirements file created")
    #install dependencies to temp packages catalog
    subprocess.call(["pip3", "install", "-r", "requirements.txt", "--target", "./packages"])
    print("dependencies installed")
    requirements.close()
    # copy project dependencies
    src = os.getcwd()
    dst = os.path.join(src, "packages")

    for item in os.listdir(src):
        if item in ("base", "utils"):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                if os.path.exists(d):
                    shutil.rmtree(d)
                shutil.copytree(s, d, symlinks=True)
            else:
                shutil.copy2(s, d, follow_symlinks=True)
    print("local dependencies copied")

    # zip packages
    zip_file = ZipFile("packages.zip" , mode='w')
    path_len = len("packages")
    for root, _ , files in os.walk("packages"):
        for file in files:
            file_path = os.path.join(root, file)
            zip_file.write(file_path , file_path[path_len :] )
    zip_file.close()
    os.remove("./requirements.txt")
    shutil.rmtree(dst)
else:
     exit("Missing pipfile.lock")
