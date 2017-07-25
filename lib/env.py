# settings.py
import os
import re

def setEnvFromFile(batFilePath, verbose = False):
    SetEnvPattern = re.compile("([A-Za-z_-]+)=(.*)", re.MULTILINE)
    SetEnvFile = open(batFilePath, "r")
    SetEnvText = SetEnvFile.read()
    SetEnvMatchList = re.findall(SetEnvPattern, SetEnvText)

    for SetEnvMatch in SetEnvMatchList:
        VarName=SetEnvMatch[0]
        VarValue=SetEnvMatch[1]
        if verbose:
            print "%s=%s"%(VarName,VarValue)
        os.environ[VarName]=VarValue