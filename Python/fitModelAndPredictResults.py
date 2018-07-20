# gensim modules
from gensim import utils
from gensim.models.doc2vec import LabeledSentence
from gensim.models import Doc2Vec
# numpy
import numpy
# random
from random import shuffle
from sklearn.linear_model import LogisticRegression
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import sklearn
from sklearn import neighbors, datasets
from sklearn import svm
from sklearn.neural_network import MLPClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.externals import joblib
import cx_Oracle

fitNewClassifier = True
model = Doc2Vec.load('./PropertyClaimModel.d2v')
vectorSize = model.docvecs.vectors_docs.shape[1]

def fitModel(positivePrefix, negativePrefixes, countOfAllModelTypes):
    positiveFitSize = getSpecificModelTypeCount(positivePrefix, countOfAllModelTypes)
    
    negativeFitSize = 0
    for negativePrefix in negativePrefixes:
        negativeFitSize += getSpecificModelTypeCount(negativePrefix, countOfAllModelTypes)
    
    test_arrays = numpy.zeros((negativeFitSize+positiveFitSize, vectorSize))
    test_labels = numpy.zeros(negativeFitSize+positiveFitSize)
    
    incrementNegative = 0
    for negativePrefix in negativePrefixes:
        currentNegativeFitSize = getSpecificModelTypeCount(negativePrefix, countOfAllModelTypes)
        for i in range(currentNegativeFitSize):
            negativeTag = negativePrefix + '_' + str(i)
            test_arrays[i+incrementNegative] = model[negativeTag]
            test_labels[i+incrementNegative] = 0
        incrementNegative += currentNegativeFitSize
    for i in range(positiveFitSize):
        positiveTag = positivePrefix + '_' + str(i)
        test_arrays[i+negativeFitSize] = model[positiveTag]
        test_labels[i+negativeFitSize] = 1

    #classifier = LogisticRegression()
    classifier = MLPClassifier(solver='adam', alpha=1e-5, hidden_layer_sizes=(100,5), random_state=1)
    classifier.fit(test_arrays, test_labels)
    return classifier

def getCountOfModelType(cursor):
    sql_statement = "select CLAIM_ML_RESULT_1, count(*) from CLM_ADAPTERDB.CLAIMS_NOTES where MODEL_TYPE = 'PROP' group by CLAIM_ML_RESULT_1"
    cursor.execute(sql_statement)
    output_list = list()
    for ClaimNotes in cursor:
        output_list.append(ClaimNotes)
    return output_list
    
def getSpecificModelTypeCount(modelType, countOfAllModelTypes):
    for modelTypeAndCount in countOfAllModelTypes:
        if modelTypeAndCount[0] == modelType:
            return modelTypeAndCount[1]
    return 0

def GetTestNotes(cursor):
    sql_statement = "select CLAIMNUMBER from CLM_ADAPTERDB.CLAIMS_NOTES where MODEL_TYPE = 'PROP' and CLAIM_ML_RESULT_1 = :1"
    cursor.execute(sql_statement,('TEST',))
    output_list = list()
    for ClaimNotes in cursor:
        output_list.append(ClaimNotes)
    return output_list

def updateNoteRowWithPrediction(cursor, connection, claimNumber, thePrediction):
    statement = "UPDATE CLAIMS_NOTES SET CLAIM_ML_RESULT_2 = :v WHERE CLAIMNUMBER = :n"
    cursor.execute(statement, {'v': thePrediction, 'n': claimNumber})
    connection.commit()
    

if __name__ == "__main__":
    connection_string = 'clm_adapterdb/Cap3eprd2@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=clms06ract.nwie.net)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=clms01ch.nsc.net)))'
    connection = cx_Oracle.connect(connection_string)
    cursor = cx_Oracle.Cursor(connection)
    
    
    #rfdmClassifier = fitModel('RFDM',['WDNR', 'HLNR', 'NRSK'], countOfAllModelTypes)
    #vowrClassifier = fitModel('VOWR',['VONR', 'NRSK'], countOfAllModelTypes)

    #sources = {'RFDM':'RFDM', 'HLNR':'HLNR', 'WDNR':'WDNR', 'PEXD':'PEXD', 'NRSK':'NRSK', 'TEST':'TEST'}

    if fitNewClassifier:
        countOfAllModelTypes = getCountOfModelType(cursor)
        theClassifier = fitModel('TRAINING',['RISK'], countOfAllModelTypes)
        joblib.dump(theClassifier, 'prop_classifier.pkl')
    else:
        theClassifier = joblib.load('prop_classifier.pkl')

    for i in GetTestNotes(cursor):
        claimNumber = str(i).split("('",1)[1].split("',)",1)[0]
        thePrediction = theClassifier.predict_proba([model['TEST_' + claimNumber]])[:,1]
        #rfdmPrediction = rfdmClassifier.predict_proba([model['TEST_' + claimNumber]])[:,1]
        #vowrPrediction = vowrClassifier.predict_proba([model['TEST_' + claimNumber]])[:,1]
        #riskTypesAndProbas = 'PEXD:' + str(pedxPrediction) + "|RFDM:" + str(rfdmPrediction) + "|VOWR:" + str(vowrPrediction)
        #riskTypesAndProbas = 'PEXD:' + str(pedxPrediction) + "|RFDM:" + str(rfdmPrediction)
        print("Updating Row: " + claimNumber + ":" + str(thePrediction))
        
        #This will update the DB with the results
        #updateNoteRowWithPrediction(cursor, connection, claimNumber, riskTypesAndProbas)

    cursor.close()
    connection.close()
    print("Connection Closed")
