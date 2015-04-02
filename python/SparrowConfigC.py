import argparse, os, re, sys

pwd = "/home/thomas/workspace/sparrow-master/"
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
    

if __name__ == "__main__":

    os.chdir(pwd)
    parser = argparse.ArgumentParser(description = "Adds IPaddresses to Sparrow Client config file", prog = "SparrowConfigC.py")
    parser.add_argument("IPs", nargs = "*")

    args = parser.parse_args() 
    
    fd = open(fileName,'r')
    content= fd.readlines()
    fd.close()	
    lineNumber = find_line(content)
    
    content[lineNumber] = "".join( (",".join((content[lineNumber][:-1], ) + tuple(args.IPs)), "\n"))
    
    fp=open(pwd + fileName,'w')
    fp.writelines(content)
    fp.close()    

