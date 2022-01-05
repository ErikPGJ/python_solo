import erikpgjohansson.solo.soar.utils
import numpy as np



def test_find_latest_versions():

    def test(itemIdArray, itemVerNbrArray, exp_bLvArray):
        expResult = np.array(exp_bLvArray, dtype=bool)

        actResult = erikpgjohansson.solo.soar.utils.find_latest_versions(
            np.array(itemIdArray,     dtype=object),
            np.array(itemVerNbrArray, dtype=int),
        )
        assert expResult.dtype == actResult.dtype
        assert expResult.shape == actResult.shape
        assert (expResult == actResult).all()
        np.testing.assert_array_equal(expResult, actResult)

    test([], [], [])
    test(['A'], [1], [1])
    test(['A', 'B'], [1, 1], [1, 1])
    test(
        ['A', 'A'], [1,2],
        [0, 1])
    test(
        ['A', 'B', 'A', 'C', 'B'], [1,2,3,5,4],
        [0,0,1,1,1])
    test(
        ['C', 'B', 'A'], [1,2,3],
        [1,1,1])



if __name__ == '__main__':
    test_find_latest_versions()
    pass
