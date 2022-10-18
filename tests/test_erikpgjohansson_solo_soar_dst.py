import erikpgjohansson.solo.soar.dst
import numpy as np
import pytest


def test_DatasetsTable():
    NA_INT1 = np.array([1, 2, 3, 4])
    # NA_INT2 = np.array([11, 12, 13])
    NA_STR1 = np.array(['1', '22', '3333', '4444'])
    # NA_STR2 = np.array(['abc', 'qwerty', 'z'])

    def test1():
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable()

        assert dst.n() is None
        with pytest.raises(KeyError):
            _ = dst['x']
        assert dst.n() is None

        with pytest.raises(AssertionError):
            _ = erikpgjohansson.solo.soar.dst.DatasetsTable({
                'x': NA_INT1,
                'y': np.array([1, 2, 3, 4, 5]),
            })
        with pytest.raises(AssertionError):
            _ = erikpgjohansson.solo.soar.dst.DatasetsTable({
                'x': NA_INT1,
                'y': np.array(99),
            })
        with pytest.raises(AssertionError):
            _ = erikpgjohansson.solo.soar.dst.DatasetsTable({
                'x': NA_INT1,
                'y': np.array([[1, 2, 3], [4, 5, 6]]),
            })

        dst = erikpgjohansson.solo.soar.dst.DatasetsTable({
            'x': NA_INT1,
            'y': NA_STR1,
        })

        na_x = dst['x']
        assert np.array_equal(na_x, NA_INT1)
        na_y = dst['y']
        assert np.array_equal(na_y, NA_STR1)
        assert dst.n() == len(NA_INT1)

    def test2():
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_INT1, 'y': NA_STR1},
        )

        assert dst.n() == len(NA_INT1)
        na_x = dst['x']
        assert np.array_equal(na_x, NA_INT1)
        na_y = dst['y']
        assert np.array_equal(na_y, NA_STR1)

    def test_index():
        dst1 = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_INT1, 'y': NA_STR1},
        )
        BI = np.array([0, 3])
        NA_INT1b = NA_INT1[BI]
        NA_STR1b = NA_STR1[BI]

        dst2 = dst1.index(BI)

        assert dst2.n() == len(NA_INT1b)
        na_x = dst2['x']
        assert np.array_equal(na_x, NA_INT1b)
        na_y = dst2['y']
        assert np.array_equal(na_y, NA_STR1b)

    test1()
    test2()
    test_index()


if __name__ == '__main__':
    test_DatasetsTable()
