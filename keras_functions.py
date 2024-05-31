from keras.saving import register_keras_serializable
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.metrics import MeanSquaredError
from tensorflow.keras import backend as K

lengh_trainning_set = 5385

@register_keras_serializable()
def r2_keras(y_true, y_pred):
    SS_res = K.sum(K.square(y_true - y_pred))
    SS_tot = K.sum(K.square(y_true - K.mean(y_true)))
    return (1 - SS_res / (SS_tot + K.epsilon()))
@register_keras_serializable()
def build_model(layers=[64, 32], input_dim=lengh_trainning_set):
    model = Sequential()
    model.add(Dense(layers[0], input_dim=input_dim, activation='relu'))
    for layer in layers[1:]:
        model.add(Dense(layer, activation='relu'))
    model.add(Dense(1, activation='linear'))
    model.compile(optimizer='adam', loss='mean_squared_error', metrics=[MeanSquaredError(), r2_keras])
    return model