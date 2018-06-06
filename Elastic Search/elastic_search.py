import json
import requests
from elasticsearch import Elasticsearch
#function for replacing extra characters
def replaceextrachar(str_vl):
    return str_vl.replace('\xef\xbb\xbf','').strip()
#function for printing the aggregations
def printaggregations(val):
    total=0
    dic_json={}
    for dic_val in val:
        print str(dic_val['key']) +"=>" + str(dic_val['doc_count']) 
        dic_json[dic_val['key']]=str(dic_val['doc_count'])
        total=total+int(dic_val['doc_count'])
    print "\nTotal " + str(total)+" docs"
    print "\nJSON dump"
    print json.dumps(dic_json)
#function for converting the scrap values to dictionary followed by json string
def getdoctordetails_json(doc_str):
    doc_dic={}
    doc_str=replaceextrachar(doc_str)
    doc_str_split=doc_str.split("|")
    doc_dic["Overview"]=doc_str_split[0]
    doc_dic["Full-name"]=doc_str_split[1]
    if "+" in doc_str_split[2]:
        doc_dic["Years-in-practice-start"]=doc_str_split[2].replace("+","").strip()
        doc_dic["Years-in-practice-end"]="above"
    else:
        doc_dic["Years-in-practice-start"]=doc_str_split[2].split("-")[0].strip()
        doc_dic["Years-in-practice-end"]=doc_str_split[2].split("-")[1].strip()
    doc_dic["Language"]=doc_str_split[3]
    tmp=doc_str_split[4].split(",")
    doc_dic["Address"]=tmp[0]
    doc_dic["City"]=tmp[1].strip().split(" ")[0]
    doc_dic["ZIP"]=tmp[1].strip().split(" ")[1]
    doc_dic["Hospital-Affiliation"]=doc_str_split[5].split("~")
    doc_dic["Specialties"]=doc_str_split[6]
    doc_dic["SubSpecialties"]=doc_str_split[7]
    doc_dic["Education-and-medical-training"]=doc_str_split[8].split("~")
    doc_dic["Certification-and-licensure"]=doc_str_split[9].split("~")
    return json.dumps(doc_dic)

agg_count=100 #aggregation count for size parameter
inputfilename="/home/manojkumarmanusai/eclipse-workspace/webscrape/details.txt" #input filename /path to file 
in_f = open(inputfilename, 'r')
lines = in_f.readlines()
i=1
#pinging the elastic webservice using request http://localhost:9200
r = requests.get('http://localhost:9200') 
if r.status_code == 200:
    #connecting to elastic search 
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    for doc in lines:
        #converting each line to json string        
        doc_json=getdoctordetails_json(doc)
        #adding json string to elastic search 
        res = es.index(index="doc-index", doc_type='doctor_data', id=i, body=doc_json)
        print(res['result']+" for id " +res['_id'])
        i+=1
    
    #finding Total number of doctors by city
    res={}
    print "\nTotal number of doctors by city"
    print "City=>Count"
    reqbody={
  "aggs": {
    "group_by": {
      "terms": {
        "field": "City.keyword",
    "size": agg_count
      }
    }
  }
}
    res = es.search(index="doc-index", body=reqbody)
    printaggregations(res['aggregations']['group_by']['buckets'])
    
    #finding Total number of doctors by specialty
    res={}
    print "\nTotal number of doctors by specialty"
    print "Specialty=>Count"
    reqbody={
  "aggs": {
    "group_by": {
      "terms": {
        "field": "Specialties.keyword",
        "size": agg_count
      }
    }
  }
}
    
    
    res = es.search(index="doc-index", body=reqbody)
    printaggregations(res['aggregations']['group_by']['buckets'])

    #finding Total number of doctors based on experience ranges
    res={}
    print "\nTotal number of doctors based on experience ranges"
    print "Year range=>Count"
    reqbody={
    "query": {
        "exists" : { "field" : "Years-in-practice-start" }      
        
    },
    "aggs": {
"year_ranges": {
  "terms": {
    "script": {
        "lang": "painless",
        "source": "int start=Integer.parseInt(doc['Years-in-practice-start.keyword'].value); if(start>20){return '20+';} if(doc['Years-in-practice-end.keyword'].value!='above'){int end=Integer.parseInt(doc['Years-in-practice-end.keyword'].value); if(start>=1 && end<=4){return '1-4'}else if(start>=5 && end<=10){return '5-10'}else if(start>=11 && end<=16){return '11-16'}else if(start>=17 && end<=20){return '17-20'}else{return 'others'}}"
      }
  }
}
   }}
    
    res = es.search(index="doc-index", body=reqbody)
    val=res['aggregations']['year_ranges']['buckets']
    year_range_dict={}
    year_range_dict['0 - 4']=0
    year_range_dict['5 - 10']=0
    year_range_dict['11 - 16']=0
    year_range_dict['17 - 20']=0
    year_range_dict['20+']=0
    for dic_val in val:
        if year_range_dict.has_key(dic_val['key']):
            year_range_dict[dic_val['key']]=int(dic_val['doc_count'])
        else:
            year_range_dict[dic_val['key']]=int(dic_val['doc_count'])
    total=0
    for key,value in year_range_dict.items():
        print str(key)+"=>"+str(value)
        total=total+value
    
    print "\nTotal " + str(total)+" docs"
    print "\nJSON dump"
    print json.dumps(year_range_dict)
    #finding Total number of doctors by ZipCode
    res={}
    print "\nTotal number of doctors by ZipCode"
    print "ZipCode=>Count"
    reqbody={              
    "aggs": {       
        "zip_types": {
            "terms": {
                "field": "ZIP.keyword",
                "size": agg_count
            }
        }    
             }
             }
    res = es.search(index="doc-index", body=reqbody)
    printaggregations(res['aggregations']['zip_types']['buckets'])
    
else:
    print "Please check elastic server on your host"

    