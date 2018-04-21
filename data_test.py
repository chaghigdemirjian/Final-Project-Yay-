import unittest
from data import *


class TestDatabase(unittest.TestCase):

    def test_yelp_table(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = 'SELECT Name FROM Yelp where city = "Ann Arbor" AND state = "MI" AND rating > 4.6'
        results = cur.execute(sql)
        result_list = results.fetchall()
        self.assertIn(('India Cafe',), result_list)
        self.assertEqual(len(result_list), 2)

        sql = '''
            SELECT Name, Rating
            FROM Yelp
            WHERE Name="Madras Masala"
            AND City = "Ann Arbor"
        '''
        results = cur.execute(sql)
        result_list = results.fetchall()
        self.assertEqual(len(result_list), 1)
        self.assertEqual(result_list[0][1], 4.0)

        conn.close()

    def test_google_table(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = '''
            SELECT Name FROM Google where city = "Ann Arbor" AND state = "MI" AND rating > 4.6
        '''
        results = cur.execute(sql)
        result_list = results.fetchall()
        self.assertIn(('India Cafe',), result_list)
        self.assertEqual(len(result_list), 1)

        sql = '''
            SELECT COUNT(distinct name)
            FROM Google where City = "Ann Arbor" and state == 'MI' and Keyword == 'Mexican'
        '''
        results = cur.execute(sql)
        count = results.fetchone()[0]
        self.assertEqual(count, 20)

        conn.close()

    def test_joins(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = '''
            SELECT G.Keyword, round(AVG(G.Rating),1) , Round(AVG(Y.Rating),1) FROM Google as G JOIN Yelp as Y
            On G.Id == Y.Id
            WHERE G.city == 'Ann Arbor' AND  G.State == 'MI'
            AND G.Keyword in ('Mexican')
            Group By G.keyword
        '''
        results = cur.execute(sql)
        result_list = results.fetchone()
        self.assertEqual(result_list[1], result_list[2])
        self.assertEqual(result_list[1], 3.8)
        self.assertEqual(len(result_list), 3)


class TestSources(unittest.TestCase):

    def cached_data_check(self):
        results = get_cached_data('Ann Arbor, MI, Indian', GOOGLE_CACHE_FILE)
        self.assertEqual(len(result_list), 30)
        self.assertIn("Namaste Flavours", results)

        results = get_cached_data('Ann Arbor, MI, Indian', YELP_CACHE_FILE)
        self.assertEqual(len(result_list), 50)
        self.assertIn("Dos Pesos Mexican Restaurant", results)

class TestFunctions(unittest.TestCase):

    def testing_this(self):
        results = plot_quantity_query('Ann Arbor', 'MI', "Indian", "Mexican","Thai", 'Google', 'off')
        self.assertEqual(results[0][2], 'Mexican')
        self.assertEqual(len(results[1]), 3)
        self.assertEqual(len(results[0]), len(results[1]))
        self.assertIsInstance(results, tuple)
        self.assertIsInstance(results[1], list)

        results2 =  average_rating_query('Ann Arbor', 'MI', "Indian", "Mexican","Thai", 'Google', 'off')
        self.assertEqual(results2[0][1], 'Mexican')
        self.assertEqual(results2[1][0], 4.0)
        self.assertEqual(len(results2[1]), 3)
        self.assertEqual(len(results2[0]), len(results2[1]))
        self.assertIsInstance(results[1], list)
        self.assertIsInstance(results, tuple)

        results3 = top_in_cat_query('Ann Arbor', 'MI',"Mexican", 'Yelp', 5, 'off')
        self.assertEqual(results3[1], "Chela's")
        self.assertIsInstance(results3, dict)

        results4 = first_plot('Ann Arbor', 'MI',"Indian", "Mexican","Thai", 'off')
        self.assertIsInstance(results4, tuple)
        self.assertEqual(len(results4),3)
        self.assertEqual(results4[1][2], 3.5)

unittest.main()
