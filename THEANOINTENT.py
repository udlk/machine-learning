import numpy
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.utils import np_utils
from keras.preprocessing.sequence import pad_sequences
from theano.tensor.shared_randomstreams import RandomStreams
import pandas as pd
numpy.random.seed(1984)
alphabet = pd.read_excel('C:/Users/uddalak/Desktop/intent.xls')
num_inputs = alphabet.shape[0]
print (alphabet.shape[0])
print (alphabet.shape[1])
alphabet1 = 'ABCDEF'
char_to_int = dict((c, i) for i, c in enumerate(alphabet1))
print (char_to_int)
int_to_char = dict((i, c) for i, c in enumerate(alphabet1))
print (int_to_char)
max_len = len(alphabet.sequence_in)
print (max_len)
dataX = []
dataY = []
for i in range(num_inputs):
	dataX.append([char_to_int[char] for char in alphabet.sequence_in[i]])
	dataY.append([char_to_int[char] for char in alphabet.sequence_out[i]])
	print (alphabet.sequence_in[i], '->', alphabet.sequence_out[i])
X = pad_sequences(dataX, maxlen=max_len, dtype='float32')
X = numpy.reshape(X, (X.shape[0], max_len, 1))
X = X / float(len(alphabet))
y = np_utils.to_categorical(dataY)
batch_size = 2
model = Sequential()
model.add(LSTM(32, input_shape=(X.shape[1], 1)))
model.add(Dense(y.shape[1], activation='softmax'))
model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
model.fit(X, y, nb_epoch=100, batch_size=batch_size, verbose=2)
scores = model.evaluate(X, y, verbose=0)
print("Model Accuracy: %.2f%%" % (scores[1]*100))
model_json = model.to_json()
with open("model.json", "w") as json_file:
    json_file.write(model_json)
# serialize weights to HDF5
model.save_weights("model.h5")
print("Saved model to disk")
import numpy
for i in range(10):
	pattern_index = numpy.random.randint(len(dataX))
	print(len(dataX))
	pattern = dataX[pattern_index]
	#print (pattern)
	x = pad_sequences([pattern], maxlen=max_len, dtype='float32')
	#print(x)
	x = numpy.reshape(x, (1, max_len, 1))
	#print(x)
	x = x / float(len(alphabet))
	#print (x)
	prediction = model.predict(x, verbose=0)
	idx = numpy.argmax(prediction)
	result = int_to_char[idx]
	seq_in = [int_to_char[value] for value in pattern]
	print (seq_in, "->", result)