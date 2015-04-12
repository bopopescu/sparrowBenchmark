import argparse, os, re, sys
from localpath import *

fileName = "sparrow.conf"

def find_line(lines):
    pattern = "^static.node_monitors = [0-9]{1,3}(\.[0-9]{1,3}){3}:[0-9]{1,5}(,[0-9]{1,3}(\.[0-9]{1,3}){3}:[0-9]{1,5})*$"
    regex = re.compile(pattern)
    lineNumber = -1;
    last = len(lines)
    while lineNumber < last:
        lineNumber += 1
        match = regex.match(lines[lineNumber])
        if match != None:
            return lineNumber
    
def addIps(ips, port=20502):
    os.chdir(pwd)
    ipsPort = [":".join((x, str(port))) for x in ips]
    fd = open(fileName,'r')
    content= fd.readlines()
    fd.close()	
    lineNumber = find_line(content)
    
    content[lineNumber] = "".join( (",".join((content[lineNumber][:-1], ) + tuple(ipsPort)), "\n"))
    
    fp=open(pwd + fileName,'w')
    fp.writelines(content)
    fp.close()

def setIps(ips, port=20502):
    os.chdir(pwd)
    ipsPort = [":".join((x, str(port))) for x in ips]
    fd = open(fileName,'r')
    content= fd.readlines()
    fd.close()	
    lineNumber = find_line(content)
    
    content[lineNumber] = "".join( ("static.node_monitors = ",) + (",".join(tuple(ipsPort)), "\n"))
    
    fp=open(pwd + fileName,'w')
    fp.writelines(content)
    fp.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = "Adds IPaddresses to Sparrow Client config file", prog = "SparrowConfigC.py")
    parser.add_argument("mode", choices = ["set","add"], nargs = 1)
    parser.add_argument("IPs", nargs = "*")
    
    args = parser.parse_args() 
    
    if args.mode[0] == "set":
        setIps(args.IPs)
    else:
        addIps(args.IPs)
