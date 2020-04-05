from __future__ import print_function
from flask import Flask, request, render_template
import os
import sys
from elasticsearch import Elasticsearch

# setting up template directory
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=TEMPLATE_DIR)
#app = Flask(__name__)




@app.route("/")
def hello():
	return TEMPLATE_DIR


def getSearchResults(keyword):
	es = Elasticsearch()
	#keyword="virus"
	res = es.search(index="twitter", body={"query": {"match" :
		{
			"tweet":keyword
		}}})
	hits = res['hits']['total']['value']
	print("Got %d Hits:" % res['hits']['total']['value'])
	results = {}
	for hit in res['hits']['hits']:
		results[hit["_source"]["user"]] = hit["_source"]["tweet"]

	return results, hits
	
def getTrendingHashTags():
	es = Elasticsearch()
	res = es.search(index="hashtag", body={"query" : 
	{
		"match_all" :{}
	},
	"sort" : {
        "count": {"order": "desc"}
    }})
	print("Got %d Hits:" % res['hits']['total']['value'])
	results = {}
	for hit in res['hits']['hits']:
		results[hit["_source"]["hashtag"]] = hit["_source"]["count"]
	return results


@app.route("/search",	 methods=['GET', 'POST'])
def rec():
	query = '' 
	if(request.method == "POST"):
		print("inside post")
		keyword = request.form.get('keyword')
		print(keyword)
		searchResults, hits = getSearchResults(keyword)
		hashtagResults = getTrendingHashTags();
		return render_template('search.html', keyword=keyword, searchResults=searchResults, hits=hits, hashtagResults=hashtagResults)
	else:
		return render_template('search.html', keyword="" ,sentiments=None, hashtagResults=None)
	

if __name__ == "__main__":
    app.run(host='127.0.0.1',port=8000, debug=False, threaded=False)



