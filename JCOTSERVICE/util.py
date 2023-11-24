





def strtofloat(string):
    if type(string) == int or type(string) == float :
        return string
    else:
        nstr = string.replace(".","").replace(",",".")
        return float(nstr)



        

