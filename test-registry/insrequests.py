import requests
import chardet

# ol-install: requests

def f(event):
    response = requests.get('https://www.baidu.com')
    return {'status': response.status_code, 'response': response.text}
    #return chardet.detect(response.content)  # {'encoding': 'utf-8', 'confidence': 0.99, 'language': ''}
    #return 'requests imported'