import numpy
#from keras.models import Sequential
#from keras.layers import Dense
#from keras.layers import LSTM
#from keras.utils import np_utils
from keras.preprocessing.sequence import pad_sequences
#from theano.tensor.shared_randomstreams import RandomStreams
from keras.models import model_from_json
import pandas as pd
numpy.random.seed(1984)
alphabet = pd.read_excel('C:/Users/uddalak/Desktop/intent_test.xls')
num_inputs = alphabet.shape[0]
alphabet1 = 'ABCDEF'
char_to_int = dict((c, i) for i, c in enumerate(alphabet1))
int_to_char = dict((i, c) for i, c in enumerate(alphabet1))
max_len = 5
dataX = []
for i in range(num_inputs):
	dataX.append([char_to_int[char] for char in alphabet.sequence_in[i]])
X = pad_sequences(dataX, maxlen=max_len, dtype='float32')
X = numpy.reshape(X, (X.shape[0], max_len, 1))
X = X / float(len(alphabet))
json_file = open('model.json', 'r')
loaded_model_json = json_file.read()
json_file.close()
loaded_model = model_from_json(loaded_model_json)
loaded_model.load_weights("model.h5")
#print("from disk")
loaded_model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
for i in range(num_inputs):
	pattern = dataX[i]
	x = pad_sequences([pattern], maxlen=max_len, dtype='float32')
	x = numpy.reshape(x, (1, max_len, 1))
	x = x / float(len(alphabet))
	prediction = loaded_model.predict(x, verbose=0)
	idx = numpy.argmax(prediction)
	result = int_to_char[idx]
	seq_in = [int_to_char[value] for value in pattern]
	print (seq_in, "->", result)
