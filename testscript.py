"""
Script that is supposed to read from a Google sheets database
in which all the groups encode their surveys of each day
Do quality control (number of entries, datatype, missing data type)
Then generate a database with one-hot encoding that can be fed to
the choice experiment statistical software. For this we need design information, with weights etc.
Perhaps for fun: add some statistics about the performance of each group?
"""
import pandas as pd
import numpy as np
from collections import OrderedDict
# Question: can we actually use google docs regarding privacy and sharing of data?
# Build our own web-form?

# Encoding of the data database is one row per respondent and as many columns as there are questions: plus additional information like name, version number, groupnumber, which area, date or even hour

def add_unique_id(original_class):
    """
    Decorator that gives an incrementally increasing unique self.id to class c each time it is initiated
    These id's are string type, to be also used as keys in a dictionary
    Unless the question is initialized with a parent question then the numbering becomes parentid.childid
    """
    uid = 1
    orig_init = original_class.__init__
    # Make copy of original __init__, so we can call it without recursion
    def __init__(self, *args, **kwargs):
        # Test whether the desired initialization is done with a parent_question
        isargquestion = [isinstance(item, Question) for item in args]
        if any(isargquestion)  or 'parent_question' in kwargs: 
            try:
                parent_question = kwargs['parent_question']
            except KeyError:
                parent_question = args[isargquestion.index(True)] 
            # Do the bookkeeping
            parent_question.nsubquestions += 1
            self.id = parent_question.id + '.' + str(parent_question.nsubquestions)
        else: # We do not have a parent and we continue with unique ids
            nonlocal uid
            self.id = str(uid)
            uid += 1
        orig_init(self, *args, **kwargs) # Call the original __init__
    original_class.__init__ = __init__ # Set the class' __init__ to the new one
    return original_class

@add_unique_id
class Question(object):
    """
    Want these to have auto-incrementing question id's and have the possibility to be a subquestion of..
    """
    def __init__(self, text: str = None, answerdtype: np.dtype = None, parent_question = None):
        """
        Requires the question text, the desired datatype of the answers
        If the question is going to be a subquestion then supply the parent
        """
        self.text = text
        self.dtype = answerdtype
        self.nodata_options = [np.nan, 9999]
        # Some bookkeeping
        self.nsubquestions = 0 # The current amount of questions that are a subquestion of this question
    
    def __repr__(self):
        return f'Question {self.id}: {self.text}'

    def check_answer(answer):
       pass # check if the answer has the correct dtype or that it has one of the accepted no_data encodings. In that case the answer should be set to None 

class Questionaire(object):

    def __init__(self):
        self.questions = OrderedDict()
        self.formdir = '/home/jsn295/ownCloud/Tenerife/testdata.xlsx'

    def __repr__(self):
        return f'{self.questions}'

    def add_question(self, question: Question):
        self.questions.update({question.id:question})

    def generate_form(self,n_respondents):
        """ Generates a database with one column per question 
        Does not apply the dtypes yet. Object dtype
        THe first row is filled with the question text
        Others (future entries) are filled with NaN
        """
        self.form = pd.DataFrame(data = None, columns = self.questions.keys(), index = ['text'] + pd.RangeIndex(1, n_respondents + 1).to_list())
        self.form.loc['text',:] = [q.text for uid,q in self.questions.items()]

    def upload_form(self):
        """
        Places the form in the owncloud
        """
        with pd.ExcelWriter(self.formdir, mode = 'w') as writer:
            self.form.to_excel(writer)

    def download_read_form(self, parse = False):
        """
        Reads the form from the cloud, parses data according desired dtypes
        First row has the column names
        Does checks on dtypes and NaNs.
        """
        self.dtypes = {uid:q.dtype for uid,q in self.questions.items()}
        if parse:
            self.form = pd.read_excel(self.formdir, skiprows = [1], index_col = 0, dtype = self.dtypes)
        else:
            self.form = pd.read_excel(self.formdir, skiprows = [1], index_col = 0, dtype = {uid:object for uid,q in self.questions.items()})

class Choice(object):

    def __init__(self, attrweights: list, scenario: int, card: int, version: int):
        self.attrs = attrweights
        self.n_attrs = len(attrweights)
        self.scenario = scenario
        self.card = card
        self.version = version

    def __repr__(self):
        return f'{self.version}.{self.card}.{self.scenario}: {self.attrs}'

class ChoiceExperiment(object):
    """
    Class to contain the information of the choice experiment:
    A set of n_versions of containing n_cards containing n_scenarios with each a choice that is defined by number of weighted attributes
    """
    def __init__(self):
        self.choices = list()
    
    def __repr__(self):
        return f'{self.choices}'

    def infer_design(self, dataframe):
        gr = dataframe.groupby(['version','card','scenario']) # Produces tuples with (index, 2d_dataframe_slice_
        for g in gr:
            self.choices.append(Choice(g[1].values.squeeze().tolist(), *g[0][::-1]))

q1 = 
q2 = Question
q21 = 
q3 = Question('do you have kids', np.bool)

survey = Questionaire()
survey.add_question(Question('how old are you?', np.int16))
survey.add_question(Question('how much do you earn?', np.uint32))
survey.add_question(Question('really that much?', np.str, parent_question = survey))
survey.add_question(Question('how much do you earn?', np.uint32))

for q in [q1,q2,q21,q211,q3]:
    survey.add_question(q)

survey.generate_form(2)



data = pd.read_excel('/home/jsn295/ownCloud/Tenerife/data.xlsx') # The version number is Q19. The choice of scenarios at each card is recorded in Q20.1 to Q20.6
version_column = 'Q19'
data.rename({version_column:'version'}, axis = 1, inplace = True)
data.index.name = 'rowid'
scenario_per_card = data.loc[:,'Q20.1':'Q20.6']
design = pd.read_excel('/home/jsn295/ownCloud/Tenerife/design.xlsx', index_col = [0,1,2], header = 0)

# Create a version with a one-hot encoding (ncolumns = ncards * nscenarios)
ncards = 6
nscenarios = 3
onehot = pd.DataFrame(data = 0, index = pd.RangeIndex(len(scenario_per_card), name = 'rowid'), columns = pd.MultiIndex.from_product([list(range(1,ncards + 1)), list(range(1,nscenarios + 1))], names = ['card','scenario']))
for card in np.unique(onehot.columns.get_level_values('card')):
    cardcol = [c for c in scenario_per_card.columns if c.endswith(str(card))][0]
    for rowid in scenario_per_card.index.get_level_values('rowid'):
        scenario_pick = scenario_per_card[cardcol].iloc[rowid]
        if scenario_pick != nscenarios + 1:
            onehot.iloc[rowid].loc[(card,scenario_pick)] = 1

# add version numbers and ids to the one-hot encoding, rebuild the index by unstacking, merge with the design weights
onehot = onehot.stack([0,1])
onehot.name = 'choice'
combined = pd.merge(onehot, data, left_index = True, right_index = True)
combined.set_index('version', append = True, drop = True, inplace = True)
combined.index = combined.index.droplevel('rowid').reorder_levels(['version','card','scenario'])
final = design.merge(combined, left_index = True, right_index = True).sort_values(['version','id','card','scenario'])
