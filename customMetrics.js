// customMetrics.js
const tf = require('@tensorflow/tfjs');

function recall(yTrue, yPred) {
    const threshold = 0.5;
    yPred = tf.cast(tf.greater(yPred, threshold), 'float32');
    
    const truePositives = tf.sum(tf.mul(yTrue, yPred));
    const actualPositives = tf.sum(yTrue);
    
    return tf.div(truePositives, tf.add(actualPositives, tf.epsilon()));
}

function precision(yTrue, yPred) {
    const threshold = 0.5;
    yPred = tf.cast(tf.greater(yPred, threshold), 'float32');
    
    const truePositives = tf.sum(tf.mul(yTrue, yPred));
    const predictedPositives = tf.sum(yPred);
    
    return tf.div(truePositives, tf.add(predictedPositives, tf.epsilon()));
}

function f1Score(yTrue, yPred) {
    const prec = precision(yTrue, yPred);
    const rec = recall(yTrue, yPred);
    
    return tf.mul(tf.div(2.0, tf.add(tf.reciprocal(prec), tf.reciprocal(rec))), 2.0);
}

module.exports = {
    recall,
    precision,
    f1Score
};