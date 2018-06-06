import requests
from bs4 import BeautifulSoup  
headers= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}
#function for removing control charcters
def strip_control_characters(input_str):
    if input_str:   
        import re         
        RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + \
                         u'|' + \
                         u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
                          (unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),
                           unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),
                           unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),
                           )
        input_str = re.sub(RE_XML_ILLEGAL, "", input_str)
        # ascii control characters
        input_str = re.sub(r"[\x01-\x1F\x7F]", "", input_str)
    return input_str
#function for generating the string that contains details of the doctor 
def getdoctordetails(url):
    #url="https://health.usnews.com/doctors/kenneth-better-530580"
    print "Started URL "+url
    page = requests.get(url,headers=headers)
    soupmain = BeautifulSoup(page.content, 'html.parser')
    
    #overview
    v_overview=soupmain.find_all(class_="block-normal clearfix")   
    soup = BeautifulSoup(v_overview[0].text, 'html.parser')
    v_overview=strip_control_characters(str(soup.contents[0])).strip()
    
    #fullname
    v_fullname=soupmain.find_all(class_="hero-heading flex-media-heading block-tight doctor-name ")   
    soup = BeautifulSoup(v_fullname[0].text, 'html.parser')
    v_fullname=strip_control_characters(str(soup.contents[0])).strip()
    v_fullname=v_fullname.replace("   ","").strip()
    
    #years in practice
    v_yersinpractice=soupmain.find_all(class_="text-large heading-normal-for-small-only right-for-medium-up")
    soup = BeautifulSoup(v_yersinpractice[1].text, 'html.parser')
    v_yersinpractice=strip_control_characters(str(soup.contents[0])).strip()
    
    #language
    v_language=soupmain.find_all(class_="text-large heading-normal-for-small-only right-for-medium-up text-right showmore")
    v_language=strip_control_characters(str(v_language[0].text)).strip()
    
    #officelocation
    v_officelocation=soupmain.find_all(class_="flex-small-12 flex-medium-6 flex-large-5")
    soup = BeautifulSoup(v_officelocation[0].text, 'html.parser')
    v_officelocation=strip_control_characters(str(soup.contents[0])).strip()
    v_officelocation=v_officelocation.split("[MAP]")[0]
    v_officelocation=v_officelocation.replace("   ","").strip()
    
    #hospitals
    v_hospitals_temp=soupmain.find_all(class_="heading-larger block-tight")
    v_hospitals=""
    for v_str in list(v_hospitals_temp): 
        soup = BeautifulSoup(v_str.text, 'html.parser')
        v_hospitals=v_hospitals + strip_control_characters(str(soup.contents[0])).strip()+"~"
    v_hospitals=v_hospitals[:-1]
    
    #specialization  
    v_specialization=soupmain.find_all('a',class_="text-large")  
    soup = BeautifulSoup(v_specialization[0].text, 'html.parser')
    v_specialization=strip_control_characters(str(soup.contents[0])).strip()
    
    #subspecialization
    v_subspecialization=soupmain.find_all('p',class_="text-large block-tight")
    soup = BeautifulSoup(v_subspecialization[0].text, 'html.parser')
    v_subspecialization=strip_control_characters(str(soup.contents[0])).strip()
    
    #education and medical training
    v_temp=soupmain.find_all(class_="block-loosest")
    soup = BeautifulSoup(str(v_temp[5]), 'html.parser')
    tmp2=soup.find_all('li',class_="block-tight")    
    v_edmt=""   
    for v_str in tmp2:  
        v_str =str(v_str.text).split("\n")[1].strip()       
        if v_str:
            v_edmt=v_edmt + strip_control_characters(v_str).strip()+"~"           
    v_edmt=v_edmt[:-1]
    
    #certification and licensure
    soup = BeautifulSoup(str(v_temp[6]), 'html.parser')
    tmp2=soup.find_all('li',class_="block-tight") 
    v_cert=""
    for v_str in tmp2:  
        v_str =str(v_str.text).split("\n")[1].strip()          
        if v_str:
            v_cert=v_cert + strip_control_characters(v_str).strip()+"~"
    v_cert=v_cert[:-1]
    print "Extracted URL "+url
    return v_overview+"|"+v_fullname+"|"+v_yersinpractice+"|"+v_language+"|"+v_officelocation+"|"+v_hospitals+"|"+v_specialization+"|"+v_subspecialization+"|"+v_edmt+"|"+v_cert+"\n"

inputfilename="/home/manojkumarmanusai/eclipse-workspace/webscrape/input.txt" #input filename /path to file
outputfilename="/home/manojkumarmanusai/eclipse-workspace/webscrape/details.txt" #output filename /path to file
in_f = open(inputfilename, 'r')
lines = in_f.readlines()
out_f = open(outputfilename, 'w')
#reading the url and writing the details of the doctor
for url in lines:
    if url:
        out=getdoctordetails(url);
        out_f.writelines(out)
        print "Completed URL "+url
print "Finished"
in_f.close()
out_f.close()
