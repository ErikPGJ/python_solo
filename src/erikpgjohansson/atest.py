# -*- coding: UTF-8 -*-
'''
#==============================================================================
Created by Erik P G Johansson 2019-01-15.

AT = Automated Testing


BOGIQ
-----
PROPOSAL: Name for every test to print (instead of numbering).
  Default is numbering which can be superseded by string.
PROPOSAL: Way of using same sequence for different functions with same
  interface, but still specify the functions once.
  Ex: diff2, diffn.
  PROPOSAL: ~Test object factory/generator.
NEED: Way of asserting properties of function arguments & return results
    without confusing failed assertion and function exception.

PROPOSAL: Function for executing all ATEST functions (methods?) in directory structure.
PROPOSAL: run_tests --> run_test_list
    PRO: Distinguish from future script that runs "all" ATEST functions.

PROPOSAL: Check type when checking for equality.
    Ex: integer vs float.

PROPOSAL: Module name "atest".
    PRO: More analogous with MATLAB package erikpgjohansson.atest.

PROPOSAL: Include some kind of automatic test TEMPLATE code.
    PROPOSAL: Use atest.ATEST().
===============================================================================
'''
import erikpgjohansson.asserts
import inspect
import numpy
import re
import time
import traceback



def run_tests(testList, quitOnFirstFail=False):
    '''
Function for automatically testing arbitrary function
Inspired by MATLAB code erikpgjohansson.utils.automatically_test_function.

ARGUMENTS
=========
testList : Iterable with "test objects" which contain one method "run()"
            which returns
            (1) None, if test OK, or
            (2) a tuple (message, variable_dict) if test failed.
              message       : String describing the type of failure.
              variable_dict : Dictionary of variables that define the test.

Created by Erik P G Johansson 2019-01-15.
'''
    '''
TODO-NEED-INFO: Possible test methods as functions?
TODO-DECISION: How handle that some printouts should maybe show pointer
               instead of object content, when using pointer comparison?
'''

    tStartSec    = time.process_time()
    nTests       = len(testList)
    nTestsFailed = 0
    for iTest, test in enumerate(testList, start=1):
        testResult = test.run()

        if testResult:
            #import pdb; pdb.set_trace()
            assert type(testResult)==tuple
            testMsg  = testResult[0]
            testVars = testResult[1]
            assert type(testVars)==dict
            nTestsFailed = nTestsFailed + 1;

            print('TEST {0} FAILED: {1:s}'.format(iTest, testMsg))

            for test_var_name in testVars:
                print(('%-25s = ' % test_var_name) + str(testVars[test_var_name]))

            if testVars['actual_exception']:
                # IMPLEMENTATION NOTE: Hack(?) to print the stack trace for
                # a stored exception.
                print('-'*100)
                try:
                    # NOTE: The current row to eventual stack trace.
                    raise testVars['actual_exception']
                except:
                    # NOTE: One can click the file paths and row numbers
                    # to go to the locations (spyder).
                    tb = traceback.format_exc()
                    print(tb)
            print('='*100)

            if quitOnFirstFail:
                print('Aborting remaining tests.')
                return
        else:
            #print('TEST {0}/{1}: OK'.format(iTest, nTests))
            #print('TEST {0:2}: OK'.format(iTest))
            pass

    if nTestsFailed == 0:
        print(f'ALL {nTests} TESTS OK.')
    else:
        print(f'{nTestsFailed} OF {nTests} TESTS FAILED.')


    tEndSec = time.process_time()
    tElapsedMs = (tEndSec-tStartSec)*1000.0
    print(f'Elapsed process time: {tElapsedMs} [ms]')



class FunctionCallTest:
    '''
Class of which an instance represents one test of a function.
For specified arguments, the function's actual return result is compared
with an expected return result.

NOTE: Uses == for comparing return results, i.e. compares values, not pointers.
NOTE: Requires exact Exception type when comparing exceptions (not subclass).


Created by Erik P G Johansson 2019-01-15.
'''
    '''
TODO-NEED-INFO: Can handle all forms of function argument lists: expansions etc?
TODO-DECISION: How handle exceptions?

PROPOSAL: Change terminology "result" (="return value")
    PRO: "result" is ambiguous.
    PROPOSAL: "return value" in text, "retval" in variables.
'''


    # Constant that can be assigned to expResult to represent that
    # expected result and actual result should not be compared.
    #
    # NOTE: (object() == object()) is false. Must use exact same object (same reference).
    #
    # IMPLEMENTATION NOTE: Must use "A is DONT_COMPARE_RESULTS" (not "==") to
    # avoid invoking overridden __eq__ methods that do not return ("scalar")
    # boolean values
    # Ex: numpy arrays.
    DONT_COMPARE_RESULTS = object()



    def __init__(self, function, args, *,
                 kwargs={},
                 expResult=DONT_COMPARE_RESULTS,
                 expExcType=None):
        '''
        ARGUMENTS
        =========
        args   : Non-string sequence of arguments to function to be tested.
        kwargs : Dictionary of keyword arguments to function to be tested.
        expResult  :
        expExcType :
        --
        Arguments that need to be assigned:
            CASE: Expect NO EXCEPTION:
                CASE: Compare expected with actual result:
                    expResult  = Expected result ("None" if no return value)
                    expExcType = None
                CASE: Do not compare expected with actual result
                    (rely on "valid_result" instead):
                    expResult  = DONT_COMPARE_RESULTS
                    expExcType = None
            CASE: Expect EXCEPTION:
                expResult  = DONT_COMPARE_RESULTS
                expExcType = Exception type
        '''
        '''
        PROPOSAL: Change the arguments list to constructor.
            Only keywords except for function, expResult?
            Only kwargs (no unnamed arguments)?
            Group args and kwargs?
        PROPOSAL: Separate keyword arguments for cases of having one/several
                  arguments (avoids sequence).
            PROPOSAL: arg, args
            PROPOSAL: arg, kwargs (keyword arguments)
        PROPOSAL: Separate test parameters, and test result (including but not only exception).

        TODO-DECISION: How handle errors in valid_result?
            PROPOSAL: Catch exceptions from valid_result.
            PROPOSAL: Expect valid_result to print/return messages (not raise exceptions).
            PROPOSAL: Expect it to return true/false.
            TODO-DECISION: Run valid_result on expected_result in the constructor, or in run()?
                PROPOSAL: run()
                    PRO: Can print error messages with the test.
                    CON: Errors not discovered until running test.
        '''

        # ASSERTIONS
        # IMPLEMENTATION NOTE: Function arguments is always a tuple here, but
        # expResult is not. A function can return both a tuple and not (e.g.
        # a string).
        erikpgjohansson.asserts.is_nonstring_sequence(args)
        assert type(kwargs) == dict
        # IMPLEMENTATION NOTE: Do permit not comparing results AND not expecting exception.
        # IMPLEMENTATION NOTE: Must not use "== FunctionCallTest.DONT_COMPARE_RESULTS".
        doCompareResult = not (expResult is FunctionCallTest.DONT_COMPARE_RESULTS)
        expectException = not (expExcType == None)
        if doCompareResult & expectException:
            raise AssertionError('Both exception and return value (result)'
                                 +' specified. Must be exactly one of those.')
        if expectException:
            assert issubclass(expExcType, Exception)

        self.function     = function
        self.args         = args
        self.kwargs       = kwargs
        self.expResult    = expResult
        self.expExcType = expExcType



    def run(self):
        '''
    Run test

    RETURN VALUES
    =============
    Either
      (a) None, or
      (b) 2-tuple
          [0] = error message to display (string)
          [1] = testData (dict).
    '''

        testData = self.__dict__

        #=====================
        # Test expected value
        #=====================
        # IMPLEMENTATION NOTE: Could be done in the constructor, but keeps it
        # here to concentrate all the tests to one location, and to take
        # advantage of the system's "framework" for displaying error messages.
        # IMPLEMENTATION NOTE: Must not use "== FunctionCallTest.DONT_COMPARE_RESULTS".
        if not (self.expResult is FunctionCallTest.DONT_COMPARE_RESULTS):
            isValidResult = self.valid_result(self.expResult)

            # IMPLEMENTATION NOTE: Important to protect againt misdefined
            # overloaded methods. Has happened that overloaded methods have
            # raised exceptions.
            assert type(isValidResult) == bool, \
                'valid_result() did not return a bool. valid_result() is misdefined.'

            if not isValidResult:
                return ('Expected result does not pass valid result assertion.', testData)

        #===============
        # Call function
        #===============
        try:
            actualResult        = self.function(*self.args, **self.kwargs)
            actualExceptionType = None
            actualException     = None
        except Exception as Exc:
            actualResult        = None
            actualExceptionType = type(Exc)
            actualException     = Exc
        testData.update({
            'actual_result':         actualResult,
            'actual_exception_type': actualExceptionType,
            'actual_exception':      actualException
            })

        #=====================
        # Analyze the outcome
        #=====================
        if actualExceptionType:
            #======================================
            # CASE: Function call raised exception
            #======================================
            if self.expExcType:
                # CASE: Test expects an exception.
                if actualExceptionType == self.expExcType:
                    # CASE: Function call raised the expected type of exception.
                    return
                else:
                    # CASE: Function call raised a exception as expected, but
                    # of the wrong type.
                    return ('Function call raised an exception as expected, '+
                            'but of the wrong type.',
                            testData)
            else:
                # CASE: Test does not expect an exception.
                    return ('Function call raised an unexpected exception.', testData)
        else:
            #================================================
            # CASE: Function call did not raise an exception
            #================================================
            if not self.valid_result(actualResult):
                return ('Actual result does not pass result assertion.', testData)

            # NOTE: Comparing all return values at once.
            # IMPLEMENTATION NOTE: Must not use "== FunctionCallTest.DONT_COMPARE_RESULTS".
            if not (self.expResult is FunctionCallTest.DONT_COMPARE_RESULTS):
                if self.results_equal(actualResult, self.expResult):
                    return
                else:
                    # USEFUL PLACE TO PUT A BREAKPOINT.
                    return ('Function call generated return result that does'
                            ' not match the expected result.', testData)



    #@staticmethod
    def valid_result(self, result):
        '''
    Reality check on result. May or may not use function arguments for this.
    Used to check both ACTUAL and EXPECTED result.
    Not necessarily intended for absolute/exact checks on result, but e.g.
    - check data types
    - non-trivial formats (variables withing variables, relationships between tuple sizes etc),
    - derive the function's input argument (sometimes trivial, while the reverse is hard).

    Can be used to effectively replace "results_equal" when expected result
    is hard to specify, but the reverse problem is easy
    (e.g. diffn, solving equations).

    IMPLEMENTATION NOTE: Method exists so that it can be overloaded if needed, e.g.
    when one can not exactly predict the result, or because it has a complicated
    format that is difficult to specify by hand.
    IMPLEMENTATION NOTE: Method is NOT STATIC so that an overloaded method can
    use instance variables, e.g. args, kwargs, and
    customized instance variables (added in subclass).


    ARGUMENTS
    =========
    result       : Function return value.
    args, kwargs : Same as submitted to constructor.

    RETURN VALUE
    ============
    result : False : Result is certainly wrong.
             True  : Result seems OK.
    '''
        '''
    PROPOSAL: Better name that does not imply crashing on error.
      PROPOSAL: testResult
      PROPOSAL: check_result
      PROPOSAL: validate_result
      PROPOSAL: valid_result
      PROPOSAL: result_assert
      PROPOSAL: *format*
      PROPOSAL: *return_value*
    PROPOSAL: Return either
      True, or
      (False, error_msg)
        '''

        return True



    @staticmethod
    def results_equal(actualResult, expResult):
        '''
    Compare actual result with expected result.

    IMPLEMENTATION NOTE: Method exists so that it can be overloaded if needed,
    e.g. because
    - there are multiple ways of representing the same result
      (e.g. order does not matter).
    - only compare parts of the results (e.g. tuple components but not others)
    - permit small numerical differences (e.g. rounding errors).
    - handle deep/shallow

    IMPLEMENTATION NOTE: Using named arguments. This is not important which
    result is which for the algorithm/testing,
    but it can be useful when inspecting variables.
    '''
        # NOTE: "==" does not always care about types.
        #   Ex: float(0) == int(0)

        # IMPLEMENTATION NOTE: Code relies on that A==B returns boolean.
        # Classes which override == (__eq__) and do NOT
        # return boolean will not work here, in particular numpy arrays.
        # Therefore having special case for numpy arrays.
        if      (type(actualResult) == numpy.ndarray) \
            and (type(expResult) == numpy.ndarray):
            return numpy.array_equal(actualResult, expResult)
        else:
            return actualResult == expResult



# ~Automatic test of code in this module.
def ATEST():

    def f1():
        return

    def f2(x):
        return x+2

    def f3(x):
        return x.copy()

    def f4():
        raise AssertionError('qwe')

    def f5(x, y=0, z=0):
        return x + y + z

    tl = []
    #tl.append(FunctionCallTest(f1, (), None))
    #tl.append(FunctionCallTest(f2, (10,), 12))
    #tl.append(FunctionCallTest(f2, (-10,), -8))

    #obj1 = [7,8,9]
    #tl.append(FunctionCallTest(f3, (obj1,), obj1))

    tl.append(FunctionCallTest(f4, (), expExcType=AssertionError))

    tl.append(FunctionCallTest(f5, args=(3,4),   expResult=7))
    tl.append(FunctionCallTest(f5, args=(3,4,1), expResult=8))
    tl.append(FunctionCallTest(f5, args=(3,),    kwargs={'z':10},               expResult=13))
    tl.append(FunctionCallTest(f5, args=(3,),    kwargs={'y':1, 'z':10},        expResult=14))
    tl.append(FunctionCallTest(f5, args=(),      kwargs={'x':2, 'y':1, 'z':10}, expResult=13))

    run_tests(tl)



def run_all_ATEST(modulePath):
    '''
Run all functions named *___ATEST in specified module, and all modules
under that module.

NOTE: Imports all related modules as a side effect.

Parameters
----------
modulePath : String. Not path.
    Ex: erikpgjohansson.utils

Returns
-------
None.
'''
    '''
PROPOSAL: Replace with pytest by changing filenaming convention.
    _ATEST --> _test
    CON: Must have separate files with automatic tests.
    CON: Must rename some files *test* files.
        Ex: test_call_OS_cmd_pipe.py
        Ex: test_soar_mirror.py
        Ex: lang_test.py
'''
    import erikpgjohansson.utils

    moduleList = erikpgjohansson.utils.get_modules(modulePath)

    for Module in moduleList:
        #modMemberNamesList = inspect.getmembers(Module)
        modMembersDict = Module.__dict__
        for modMemberName, modMemberObj in modMembersDict.items():
            if inspect.isfunction(modMemberObj):
                #print(Module.__name__ + '.' + modMemberName)   # DEBUG

                if re.search('___ATEST$', modMemberName):
                    print(Module.__name__ + '.' + modMemberName)

                    # EXECUTE FUNCTION
                    modMemberObj()
