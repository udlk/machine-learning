# load json and create model
import numpy
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.utils import np_utils
from keras.preprocessing.sequence import pad_sequences
from keras.models import model_from_json
from theano.tensor.shared_randomstreams import RandomStreams
import pandas as pd
json_file = open('model.json', 'r')
loaded_model_json = json_file.read()
json_file.close()
loaded_model = model_from_json(loaded_model_json)
print (loaded_model)
# load weights into new model
loaded_model.load_weights("model.h5")
print("Loaded model from disk")
loaded_model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
numpy.random.seed(1984)
alphabet = pd.read_excel('C:/Users/uddalak/Desktop/intent_test.xls')
num_inputs = alphabet.shape[0]
print (alphabet.shape[0])
alphabet1 = 'ABCDEF'
char_to_int = dict((c, i) for i, c in enumerate(alphabet1))
print (char_to_int)
int_to_char = dict((i, c) for i, c in enumerate(alphabet1))
print (int_to_char)
max_len = 5
print (max_len)
dataX = []
for i in range(num_inputs):
	dataX.append([char_to_int[char] for char in alphabet.sequence_in[i]])
	#dataY.append([char_to_int[char] for char in alphabet.sequence_out[i]])
	#print (alphabet.sequence_in[i], '->', alphabet.sequence_out[i])
X = pad_sequences(dataX, maxlen=max_len, dtype='float32')
X = numpy.reshape(X, (X.shape[0], max_len, 1))
X = X / float(len(alphabet))

loaded_model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
for i in range(num_inputs):
    pattern_index = numpy.random.randint(len(dataX))
    print (pattern_index)
    pattern = dataX[pattern_index]
    print (pattern)
    x = pad_sequences([pattern], maxlen=max_len, dtype='float32')
    print(x)
    x = numpy.reshape(x, (1, max_len, 1))
    print(x)
    x = x / float(len(alphabet))
    print (x)
    prediction = loaded_model.predict(x, verbose=0)
    idx = numpy.argmax(prediction)
    result = int_to_char[idx]
    seq_in = [int_to_char[value] for value in pattern]
    print (seq_in, "->", result)


