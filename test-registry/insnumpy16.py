# ol-install: numpy

import numpy

def f(event):
    #return dir(numpy)
    #return {'name': numpy.__name__}
    return {'version': numpy.__version__}
    #return {'result': int(numpy.sum([0.5, 1.5]))}
    #return {'result': int(numpy.array(event).sum())}
