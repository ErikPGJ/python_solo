import erikpgjohansson.solo.soar.dst
import numpy as np
import pytest


def test_DatasetsTable():
    NA_1 = np.array([1, 2, 3, 4])
    NA_2 = np.array(['1', '22', '3333', '4444'])

    def test1():
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable()

        assert dst.n() is None
        with pytest.raises(KeyError):
            _ = dst['x']
        assert dst.n() is None

        dst['x'] = NA_1

        with pytest.raises(KeyError):
            dst['x'] = NA_1
        with pytest.raises(AssertionError):
            dst['y'] = np.array([1, 2, 3, 4, 5])
        with pytest.raises(AssertionError):
            dst['y'] = np.array(99)
        with pytest.raises(AssertionError):
            dst['y'] = np.array([[1, 2, 3], [4, 5, 6]])

        dst['y'] = NA_2

        na_x = dst['x']
        assert np.array_equal(na_x, NA_1)
        na_y = dst['y']
        assert np.array_equal(na_y, NA_2)
        assert dst.n() == len(NA_1)

    def test2():
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_1, 'y': NA_2},
        )

        assert dst.n() == len(NA_1)
        na_x = dst['x']
        assert np.array_equal(na_x, NA_1)
        na_y = dst['y']
        assert np.array_equal(na_y, NA_2)

    def test_index():
        dst1 = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_1, 'y': NA_2},
        )
        BI = np.array([0, 3])
        NA_1b = NA_1[BI]
        NA_2b = NA_2[BI]

        dst2 = dst1.index(BI)

        assert dst2.n() == len(NA_1b)
        na_x = dst2['x']
        assert np.array_equal(na_x, NA_1b)
        na_y = dst2['y']
        assert np.array_equal(na_y, NA_2b)

    test1()
    test2()
    test_index()


if __name__ == '__main__':
    test_DatasetsTable()
