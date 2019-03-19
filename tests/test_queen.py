import unittest
from code.handlers import QueenBee

class TestWorker(unittest.TestCase):
    """
    Test out the WorkerBee class
    """
    def test_currentdir(self):
        """Test that currentdir attribute returns correct cwd"""
        testdir = './test_dir/'
        wb = QueenBee(testdir)
        self.assertEqual(wb.currentdir, testdir)
        
    def test_fail_directset_currentdir(self):
        testdir = './test_dir/'
        wb = QueenBee(testdir)
        with self.assertRaises(AttributeError):
            wb.currentdir = 'this_should_fail'
        self.assertEqual(wb.currentdir, testdir)
        
    def test_chdir(self):
        testdir = 'p1/p2/p3/test_dir'
        target = '../../../p4/p5/test_dir2'
        exp_result = 'p1/p4/p5/test_dir2/'
        wb = QueenBee(testdir)
        wb.chdir(target)
        self.assertEqual(wb.currentdir, exp_result)
    
    
if __name__ == '__main__':
    unittest.main()