import chardet

# ol-install: chardet

def f(event):
    msg = 'chardet imported'.encode('utf-8')
    return chardet.detect(msg) # {'encoding': 'utf-8', 'confidence': 0.99, 'language': ''}
    #return {'version': chardet.__version__, 'status': "chardet imported"}