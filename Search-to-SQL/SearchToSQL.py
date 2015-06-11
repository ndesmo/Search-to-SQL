from py2neo import Graph, Relationship
import requests
import pyodbc
import re

from simplejson.scanner import JSONDecodeError 

API_KEY = 'BigHugeLabs API key'

DB_URI = 'https://user:password@localhost:7473/db/data'

SQL_SERVER = 'sqlservername'
SQL_DATABASE = 'sqldbname'

class SQLTerms():
    
    def __init__(self):
        
        self.sql_terms = ['select',
                          'update',
                          'insert',
                          'delete',
                          'union',
                          'merge',
                          'sum',
                          'order',
                          'where',
                          'from',
                          'into',
                          'group',
                          'and']

class SyntaxGraph():
    
    """
    The aim of this class is to find associated words to database syntax.
    A user will input a sentence, and these associations will be used to
    find the correct SQL statement to execute in the database.
    
    The relations between words are modelled as a graph. The nodes of the 
    graph are the words, and the edges (relationships) between nodes
    represent when a word means another word (e.g. is a synonym).
    
    The graph is "seeded" using a set of database syntax words, finding 
    synonyms/related words to these initial words using a call to a
    thesaurus API.
    
    The graph is then "grown" from the resulting synonyms using subsequent
    API calls, in a recursive fashion.
    
    When a user enters a sentence, this graph will be used to find 
    database syntax words which are within a certain "degree of 
    separation" from each word in the sentence, in an attempt to 
    start building a SQL query from this sentence.
    """
    
    def __init__(self, seed_words=None, seed_mappings=None):
        
        self.sql_terms = SQLTerms().sql_terms
        
        self.graph = Graph(DB_URI)
        self.tx = self.graph.cypher.begin()
        
        self.seed_mappings = seed_mappings or {'where': ['filter', 'for', 'during'],
                                               'from': ['source', 'in'],
                                               'into': ['toward', 'within', 'inside'],
                                               'group':['by'],
                                               'and': ['with']}
        
        self.seed_words = seed_words or [x for x in self.sql_terms if x not in self.seed_mappings]
    
        self.seed_words.extend([x for x in self.seed_mappings.iterkeys()])
        
        self.exclude_words = ['display']
        
    def seed(self, reset=False):
        
        print 'Seeding graph'
        
        if reset:
            self.graph.delete_all()
        
        for word in self.seed_words:
            if not self.already_called(word):
                self.add_synonyms(word)
            if word in self.seed_mappings:
                print 'Mapping %s to %s' % ( ','.join(self.seed_mappings[word]), word )
                base = self.graph.merge_one('Word', 'name', word)
                synonyms = [self.graph.merge_one('Word', 'name', x) for x in self.seed_mappings[word]]
                [self.graph.create_unique(Relationship(base, 'MEANS', synonym)) for synonym in synonyms]
                [self.graph.create_unique(Relationship(synonym, 'MEANS', base)) for synonym in synonyms]
            
                
    def grow(self, levels=1):
        
        print 'Levels left: %d' % levels
        
        query = ''' MATCH (w:Word)
                    WHERE NOT HAS (w.called)
                    RETURN w.name
                '''
        
        results = self.graph.cypher.execute(query)     
        
        for word in results:
            self.add_synonyms(word['w.name'])
            
        if levels > 1:
            self.grow(levels-1)
                
            
    def already_called(self, word):
        
        if len (self.graph.cypher.execute('''MATCH (w:Word)
                                             WHERE w.name = '%s'
                                               AND HAS (w.called)
                                             RETURN w.name 
                                          ''' % word) ) > 0:
            return True
        
    def update_set_called(self, word):
        
        word_node = self.graph.merge_one('Word', 'name', word)
        word_node.properties['called'] = 1
        word_node.push()
        
    def add_synonyms(self, word):
                                     
        url = 'http://words.bighugelabs.com/api/2/%s/%s/json' % (API_KEY, word)
        print url
        
        response = requests.get(url)
        
        try:
            data = response.json()
        except JSONDecodeError:
            self.update_set_called(word)
            return
        
        if 'verb' in data:
            for key in data['verb']:
                # Synonyms: words are all interrelated (connected graph)
                if key == 'syn':
                    
                    synonyms = [word]
                    synonyms.extend([x for x in data['verb'][key] if ' ' not in x])
                    
                    nodes = [self.graph.merge_one('Word', 'name', x) for x in synonyms]
                    [self.graph.create_unique(Relationship(i, 'MEANS', j)) for j in nodes for i in nodes if i!=j]
                    
                # Similar / user defined words: words are related both ways between root and related words (both direction)
                elif key in ('sim', 'usr'):
                    
                    related_words = [word]
                    related_words.extend([x for x in data['verb'][key] if ' ' not in x])
                    
                    nodes = [self.graph.merge_one('Word', 'name', x) for x in related_words]
                    [self.graph.create_unique(Relationship(nodes[i], 'MEANS', nodes[j])) for j in range(len(nodes)) for i in range(len(nodes)) if (i+j>0 and i*j==0)]
                    
                # Related words: words are related only from root to related word (one direction)
                elif key == 'rel':
                    
                    related_words = [word]
                    related_words.extend([x for x in data['verb'][key] if ' ' not in x])
                    
                    nodes = [self.graph.merge_one('Word', 'name', x) for x in related_words]
                    [self.graph.create_unique(Relationship(nodes[0], 'MEANS', nodes[i])) for i in range(1, len(nodes))]
            
        self.update_set_called(word)
        
    def replace_word(self, word, max_degree_separation=2):
        
        if word in self.seed_words or word in self.exclude_words: return word
        
        replacement_candidates = []
        
        for seed_word in self.seed_words:
        
            query = '''MATCH p=shortestPath((w:Word{name:"%s"})-[*]-(n:Word{name:"%s"}))
                       RETURN length(p), n.name
                    ''' % (word, seed_word)
                    
            results = self.graph.cypher.execute(query)
            
            try:
                replacement_candidates.append(min([(row['length(p)'], row['n.name']) for row in results]))
            except ValueError:
                pass

        if len(replacement_candidates) > 0:
            replacement = min(replacement_candidates)
            if replacement[0] <= max_degree_separation:
                return replacement[1]
        
    def replace_text(self, text):
        
        pattern = re.compile('[\W_]+')
        cleaned = []
        replacements = []
        
        for word in text.split():
            cleaned_word = pattern.sub('', word)
            
            if cleaned_word not in [x[0] for x in cleaned]:
                cleaned.append([cleaned_word, self.replace_word(cleaned_word)])
            
            replacements.append(self.replace_word(cleaned_word) or cleaned_word)
        
        return ' '.join(replacements)
            
class ProcessQueryText():
    
    def __init__(self, query):
        
        obj = SQLTerms()
        [setattr(self, a, getattr(obj, a)) for a in dir(obj) if not a.startswith('__') and not callable(getattr(obj,a))]
        self.remove_words = ['data']
        #self.sql_terms = SQLTerms().sql_terms
        
        self.query = query
        self.run()
        
    def run(self):
        
        self.connect_to_database()
        self.add_initial_select()
        self.remove_certain_words()
        self.detect_dates()
        self.detect_countries()
        self.deduplicate_terms()
        #self.replace_extra_wheres()
        self.remove_whitespace()
        self.parse()
        self.merge_wheres()
        self.query_database()
    
        
    def add_initial_select(self):
        
        if 'select' not in self.query:
            self.query = 'select ' + self.query
            
    def remove_certain_words(self):
        
        self.query = re.sub('(%s)' % ')|('.join(self.remove_words), '', self.query)
        self.remove_whitespace() 
            
    def detect_dates(self):
        
        def prefix_year(matchobj):
            return ' where year = %s' % re.search('[12][019][0-9][0-9]', matchobj.group(0)).group(0)
        
        self.query = re.sub('((year)?\s*?=?\s*?\'?)[12][019][0-9][0-9]', prefix_year, self.query)
        
        months = ['january','february','march','april','may','june','july','august','september','october','november','december']
        
        def prefix_month(matchobj):
            return ' where month = \'%s\'' % re.search('|'.join(months), matchobj.group(0)).group(0)
        
        self.query = re.sub('|'.join([r'((month)?\s*?=?\s*?\'?)?%s' % month for month in months]), prefix_month, self.query)
            
    def detect_countries(self): 
        
        self.get_country_names()
    
        self.country = ''
            
        def prefix_country(matchobj):
            return ' where %s = \'%s\'' % (self.country_column, re.search(self.country.lower(), matchobj.group(0)).group(0))
        
        for country in self.countries:
            self.country = country
            self.query = re.sub(r'((country|country\sname)?\s*?=?\s*?\'?)?%s' % country.lower(), prefix_country, self.query)
        
    def connect_to_database(self):
        
        self.conn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes' % (SQL_SERVER,SQL_DATABASE),
                                       unicode_results=True,
                                       autocommit=False)
        
        self.cursor = self.conn.cursor()
        
    def get_country_names(self):
        
        SQL = ''' SELECT COLUMN_NAME
                       , TABLE_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_NAME LIKE '%Dim%Country%'
                     AND REPLACE(COLUMN_NAME, '_', '') IN ('Country', 'CountryName')
              '''
        
        self.country_column, table = [row for row in self.cursor.execute(SQL).fetchall()][0]
        
        SQL = ''' SELECT DISTINCT %s
                    FROM %s
              ''' % (self.country_column, table)
        
        self.countries = [row[0] for row in self.cursor.execute(SQL).fetchall()]
    
    def deduplicate_terms(self):
            
        def deduplicate_match(matchobj):
            return matchobj.group(0).split()[0]
            
        self.query = re.sub('|'.join([r'(%s)(\s(%s))+'% (sql_term, sql_term) for sql_term in self.sql_terms]), deduplicate_match, self.query)
        
    def replace_extra_wheres(self):
        
        try:
            idx = self.query.index('where') + len('where')
            self.query = self.query[:idx] + self.query[idx:].replace('where', 'and')
        except ValueError:
            pass
        
        self.deduplicate_terms()
                
    def remove_whitespace(self):
        
        self.query = re.sub('\s\s+', ' ', self.query) 
        
    def parse(self):
        
        self.sql_params = dict((sql_term, [x.strip() for x in re.findall(r'(?<=%s).+?(?=%s)' % (sql_term,'|'.join(self.sql_terms)), self.query + ' select')]) for sql_term in self.sql_terms if sql_term in self.query)
        select_columns = set([])
        for item in self.sql_params['select']:
            for value in item.split():
                select_columns.add(value)
        self.sql_params['select'] = list(select_columns)
        print self.sql_params
        
    def merge_wheres(self):
        
        sql_wheres = dict((x.lower(), x.split('=')[0].strip().lower()) for x in self.sql_params['where'])
        skips = []
        replaced_sql_wheres = []
        
        for sql_where in self.sql_params['where']:
            sql_where_column = sql_where.split('=')[0].strip().lower()
            if sql_where_column in skips: continue
            replaced_sql_where = sql_where
            if sql_where_column in dict((key, value) for key, value in sql_wheres.items() if key != sql_where.lower()).values():
                replaced_sql_where_values = [x.split('=')[1].strip().lower() for x in sql_wheres.keys() if x.split('=')[0].strip().lower() == sql_where_column]
                replaced_sql_where = "%s IN ('%s')" % (sql_where_column, "','".join(replaced_sql_where_values))
                skips.append(sql_where_column)
            replaced_sql_wheres.append(replaced_sql_where)
        
        self.sql_params['where'] = replaced_sql_wheres
        
    def query_database(self):
        
        from_part = ''
        if 'from' in self.sql_params:
            from_part = 'WHERE ' + ' AND '.join(["f.name LIKE '%%%s%%'" % sql_from for sql_from in self.sql_params['from']])
        
        SQL = ''' WITH The_Data AS (
                  SELECT f.name AS Fact_Table
                       , STUFF( ( SELECT ',SUM(f.[' + fc.name + '])'
                                    FROM sys.columns fc
                                   WHERE fk.parent_object_id = fc.object_id
                                     AND fc.name IN ('%s')
                                  FOR XML PATH ('') ), 1, 1, '') AS Fact_Column
                       , STUFF( ( SELECT ',f.[' + fc.name + ']'
                                    FROM sys.columns fc
                                   WHERE fk.parent_object_id = fc.object_id
                                     AND fc.name IN ('%s')
                                  FOR XML PATH ('') ), 1, 1, '') AS Fact_Column_Where
                       , d.name AS Dim_Table
                       , STUFF( ( SELECT ',[' + dc.name + ']'
                                    FROM sys.columns dc
                                   WHERE fk.referenced_object_id = dc.object_id
                                     AND ( dc.name IN ('%s') )
                                  FOR XML PATH ('') ), 1, 1, '') AS Dim_Column
                       , STUFF( ( SELECT ',[' + dc.name + ']'
                                    FROM sys.columns dc
                                   WHERE fk.referenced_object_id = dc.object_id
                                     AND ( dc.name IN ('%s') )
                                  FOR XML PATH ('') ), 1, 1, '') AS Dim_Column_Where
                       , jfc.name AS Fact_Join_Column
                       , jdc.name AS Dim_Join_Column
                    FROM sys.foreign_key_columns fk
                    JOIN sys.tables f
                      ON fk.parent_object_id = f.object_id
                    JOIN sys.tables d
                      ON fk.referenced_object_id = d.object_id
                    JOIN sys.columns jfc
                      ON fk.parent_object_id = jfc.object_id
                     AND fk.parent_column_id = jfc.column_id
                    JOIN sys.columns jdc
                      ON fk.referenced_object_id = jdc.object_id
                     AND fk.referenced_column_id = jdc.column_id
                  %s
                  ), Filtered AS (
                  SELECT DISTINCT Fact_Table, Fact_Column
                       , CASE WHEN Dim_Column IS NOT NULL OR Dim_Column_Where IS NOT NULL THEN Dim_Table END AS Dim_Table
                       , Dim_Column
                       , CASE WHEN Dim_Column IS NOT NULL OR Dim_Column_Where IS NOT NULL THEN Fact_Join_Column END AS Fact_Join_Column
                       , CASE WHEN Dim_Column IS NOT NULL OR Dim_Column_Where IS NOT NULL THEN Dim_Join_Column END AS Dim_Join_Column
                       , Fact_Column_Where
                       , Dim_Column_Where
                    FROM The_Data
                   WHERE Fact_Column IS NOT NULL
                  ), Facts AS (
                  SELECT Fact_Table, Fact_Column, COUNT(Dim_Table) AS Dims_Count
                    FROM Filtered
                   GROUP BY Fact_Table, Fact_Column
                  )
                  SELECT 'SELECT ' + Fact_Column + '
                            FROM [' + Fact_Table + '] f
                         ' + CASE WHEN Dims_Count > 0 THEN
                                   STUFF( ( SELECT ' JOIN [' + Dim_Table + '] ON f.[' + Fact_Join_Column + '] = [' + Dim_Table + '].[' + Dim_Join_Column + ']'
                                             FROM Filtered fi
                                            WHERE fi.Fact_Table = f.Fact_Table
                                           FOR XML PATH ('') ), 1, 1, '')
                                  ELSE '' END + '
                        ' + CASE WHEN Dims_Count > 0 THEN 
                                  STUFF( ( SELECT ' WHERE [' + Dim_Table + '].' + Dim_Column_Where + '' 
                                             FROM Filtered fi
                                            WHERE fi.Fact_Table = f.Fact_Table
                                           FOR XML PATH ('') ), 1, 1, '')
                                  ELSE '' END
                    FROM Facts f
              ''' % ("','".join(self.sql_params['select']),
                     "','".join([x.split('=')[0].strip().split('IN')[0].strip() for x in self.sql_params['where']]),
                     "','".join(self.sql_params['select']),
                     "','".join([x.split('=')[0].strip().split('IN')[0].strip() for x in self.sql_params['where']]),
                     from_part
                     )
        
        queries = [x[0] for x in self.cursor.execute(SQL).fetchall()]
        
        def replace_where_part(matchobj):
            where_part = matchobj.group(0).lower()
            where_cols = dict(('[%s]' % x.split('=')[0].strip().split('IN')[0].strip().lower(), x) for x in self.sql_params['where'] if x.split('=')[0].strip().split('IN')[0].strip().lower() in where_part)
            for where_col in where_cols:
                where_part = where_part.replace(where_col, where_cols[where_col])
            where_part = re.sub(r',(?!\')', ' AND ', where_part)           
            where_part = re.sub(r'(?!^)where\s', 'and ', where_part)
            return where_part
        
        for query in queries:
            query = re.sub(r'WHERE.+', replace_where_part, query, flags=re.IGNORECASE).replace("''","'")
            print query
            results = [row for row in self.cursor.execute(query)]
            print results

if __name__ == '__main__':
    
    g = SyntaxGraph()
    
    g.seed()
    g.grow()
    
    replaced = g.replace_text("clicks impressions in display data january usa 2015 united states february united kingdom")

    p = ProcessQueryText(replaced)
    
    