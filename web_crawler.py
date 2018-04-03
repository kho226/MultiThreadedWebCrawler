'''
    Author: Kyle Ong
    Date: 03/30/2018

    Multi-threaded web crawler

    class MultiThreadedCrawler:
        - base_url ~> string
        - thread_pool  ~> ThreadPoolExecutor
        - visited_pages ~> set
        - queue ~> Queue
        - headers ~> struct
        - add_links_to_queue(self,html)
        - get_response(self,url)
        - response_callback(self,response)
        - crawl_info(self,result)
        - run_scraper(self)

'''

import requests 
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import sys
import time


class MultiThreadedCrawler:

    def __init__(self,url):
        self.base_url = url
        self.root_url = "{0}://{1}".format(urlparse(self.base_url).scheme,urlparse(self.base_url).netloc)
        self.thread_pool = ThreadPoolExecutor(max_workers = 3)
        self.visited_pages = set([])
        self.queue = Queue()
        self.queue.put(self.base_url)
        self.headers = {
        'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
        }

    def add_links_to_queue(self,result):
        '''
            Author: Kyle Ong
            Date: 03/30/2018

            finds all <a href> tags in html and pushes to queue

            type: string: html
    
        '''

        html = result.text
        soup = BeautifulSoup(html,'lxml')
        a_tags = soup.find_all('a',href=True)
        for tag in a_tags:
            url = tag['href']
            if url.startswith('/') or url.startswith(self.root_url):
                url = urljoin(self.root_url,url)
                if url not in self.visited_pages:
                    self.queue.put(url)

            



    def get_response(self, url):
        '''
            Author : Kyle Ong
            Date: 03/20/2018

            returns a request object of url

            type: string: url

        '''
        try:
            response = requests.get(url,headers=self.headers,timeout=(2,20))
            return response
        except(requests.exceptions.RequestException):
            print(requests.exceptions.RequestException)
            


    def response_callback(self,response):
        '''
            Author: Kyle Ong
            Date: 03/30/2018

            callback executed when a job exits threadPool

            type: r: Request 

        '''
        result = response.result()
        if not result or result.status_code != 200:
            print (result.status_code)
           
        
        self.add_links_to_queue(result)
        self.print_links(result)


    def print_links(self,result):
        '''
            Author: Kyle Ong
            Date: 03/30/2018

            type: Reqeuest: result

        '''
        html = result.text
        soup = BeautifulSoup(html,'lxml')
        a_tags = soup.find_all('a',href=True)
        for i in range(1,len(a_tags),1):
            url = a_tags[i]['href']
            if url.startswith('http') or url.startswith("https"):
                url = urljoin(self.root_url,url)
                print("\t{0}".format(url))

    def run_crawler(self):
        '''   
            Author: Kyle Ong
            Date: 03/30/2018

            function to perform multithreaded crawl

        '''
        while True:
            try:
                url = self.queue.get(timeout = 5)
                if url not in self.visited_pages:
                    self.visited_pages.add(url)
                    print (url)
                    job = self.thread_pool.submit(self.get_response,url)
                    job.add_done_callback(self.response_callback)
            except Empty:
                print("Queue is empty")
                sys.exit()
            except Exception as e:
                print (e)
               






if __name__ == '__main__':
    url = sys.argv[1]
    C = MultiThreadedCrawler(url)
    C.run_crawler()
    