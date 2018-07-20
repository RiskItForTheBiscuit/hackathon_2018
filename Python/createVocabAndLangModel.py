from gensim import utils
from gensim.models.doc2vec import TaggedDocument
from gensim.models import Doc2Vec
from random import shuffle
import cx_Oracle

class LabeledLineSentence(object):
    def __init__(self, sources):
        self.sources = sources
        self.DbConnection = cx_Oracle.connect('username/password@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=clms06ract.nwie.net)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=clms01ch.nsc.net)))')
        self.CursorVar = self.DbConnection.cursor()
        print("Connection Opened")
        
        flipped = {}
        
        # make sure that keys are unique
        for key, value in sources.items():
            if value not in flipped:
                flipped[value] = [key]
            else:
                raise Exception('Non-unique prefix encountered')
    
    def to_array(self):
        self.sentences = []
        for source, prefix in self.sources.items():
            print("current prefix: " + str(prefix))
            for item_no, line in enumerate(GetNotes(self.CursorVar,source)):
                if prefix == 'TEST':
                    #print(prefix + '_%s' % line[0])
                    self.sentences.append(TaggedDocument(utils.to_unicode(line[1].read()).split(), [prefix + '_%s' % line[0]]))
                else:
                    self.sentences.append(TaggedDocument(utils.to_unicode(line[1].read()).split(), [prefix + '_%s' % item_no]))
        return self.sentences
    
    def sentences_perm(self):
        shuffle(self.sentences)
        return self.sentences
    
    def closeDBConnection(self):
        self.CursorVar.close()
        self.DbConnection.close()

def GetNotes(cursor_var, model_type):
    sql_statement = "select CLAIMNUMBER, NOTES from CLM_ADAPTERDB.CLAIMS_NOTES WHERE MODEL_TYPE = 'PROP' and CLAIM_ML_RESULT_1 = :1"
    cursor_var.execute(sql_statement,(model_type,))
    output_list = list()
    for ClaimNotes in cursor_var:
        output_list.append(ClaimNotes)
    return output_list

if __name__ == "__main__":
    sources = {'TRAINING':'TRAINING', 'TEST':'TEST', 'RISK':'RISK'}
    sentences = LabeledLineSentence(sources)

    model = Doc2Vec(min_count=5, window=10, vector_size=500, sample=1e-5, negative=10, workers=8, alpha=0.025, min_alpha = 0.025, dm=1)
    #print("buliding vocab")
    model.build_vocab(sentences.to_array())
    
    for epoch in range(10):
        print("Starting epoch ")
        model.train(sentences.sentences_perm(), total_examples=model.corpus_count, epochs=model.epochs)
        model.alpha -= 0.002 # decrease the learning rate
        model.min_alpha = model.alpha # fix the learning rate, no deca
    
    model.save('./PropertyClaimModel.d2v')
    print("Model Saved")
    sentences.closeDBConnection()
    print("Connection Closed")
