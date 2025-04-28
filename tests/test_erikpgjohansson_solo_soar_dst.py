import erikpgjohansson.solo.soar.dst
import numpy as np
import pytest


def test_DatasetsTable():
    NA_INT1 = np.array([1, 2, 3, 4])
    NA_INT2 = np.array([11, 12, 13])
    NA_STR1 = np.array(['1', '22', '3333', '4444'])
    NA_STR2 = np.array(['abc', 'qwerty', 'z'])

    def test1():
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable()

        assert dst.n_rows is None
        with pytest.raises(KeyError):
            _ = dst['x']
        assert dst.n_rows is None

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
        assert dst.n_rows == len(NA_INT1)

    def test2():
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_INT1, 'y': NA_STR1},
        )

        assert dst.n_rows == len(NA_INT1)
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

        assert dst2.n_rows == len(NA_INT1b)
        na_x = dst2['x']
        assert np.array_equal(na_x, NA_INT1b)
        na_y = dst2['y']
        assert np.array_equal(na_y, NA_STR1b)

    def test_add():
        dst1 = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_INT1, 'y': NA_STR1},
        )
        dst2 = erikpgjohansson.solo.soar.dst.DatasetsTable(
            {'x': NA_INT2, 'y': NA_STR2},
        )
        dst3 = dst1 + dst2
        assert dst3.n_rows == dst1.n_rows + dst2.n_rows
        assert np.array_equal(dst3['x'], np.concatenate((NA_INT1, NA_INT2)))
        assert np.array_equal(dst3['y'], np.concatenate((NA_STR1, NA_STR2)))

    test1()
    test2()
    test_index()
    test_add()


def test_log_DST():
    '''Test if crashes, and for manually inspecting the log output.'''
    def test(ls_file_size, ls_begin_time_fn, ls_instrument, processing_level):
        dc = {
            'file_size':
                np.array(ls_file_size, dtype='int64'),
            'begin_time_FN':
                np.array(ls_begin_time_fn, dtype='datetime64[ms]'),
            'instrument':
                np.array(ls_instrument, dtype=object),
            'processing_level':
                np.array(processing_level, dtype=object),
        }
        dst = erikpgjohansson.solo.soar.dst.DatasetsTable(dc)
        erikpgjohansson.solo.soar.dst.log_DST(dst, '<title string>')

    DT64_NAT = np.datetime64('NaT')
    DT64_1 = np.datetime64('2020-01-01T00:00:00')
    DT64_2 = np.datetime64('2021-01-01T00:00:00')

    test([], [], [], [])
    test([1e6], [DT64_1], ['MAG'], ['L1'])
    test([1e6], [DT64_NAT], ['MAG'], ['L1'])
    test(
        [1e6, 10e6, 100e6, 1000e9],
        [DT64_1, DT64_1, DT64_2, DT64_NAT],
        ['MAG', 'EPD', 'EPD', 'EPD'],
        ['L1', 'L2', 'L2', 'L2'],
    )
