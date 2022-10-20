import erikpgjohansson.solo.soar.tests as tests
import os
import tempfile


def test_assert_FS(tmp_path):
    test_dir = os.path.join(tmp_path, 'test_A')
    os.makedirs(test_dir)
    tests.assert_FS(test_dir, {})

    test_dir = os.path.join(tmp_path, 'test_B')
    os.makedirs(os.path.join(test_dir, 'dir1/dir2'))
    tests.create_file(os.path.join(test_dir, 'dir1/file1'), 22)
    tests.create_file(os.path.join(test_dir, 'dir1/dir2/file2'), 11)
    tests.assert_FS(
        test_dir, {
            'dir1': {
                'file1': 22,
                'dir2': {
                    'file2': 11,
                },
            },
        },
    )


def test_setup_FS_assert_FS(tmp_path):
    '''Test that setup_FS() and assert_FS() are consistent with each other.'''
    dp = tests.DirProducer(tmp_path)

    def test_eq(dict_objs):
        test_dir = dp.get_new_dir()

        tests.setup_FS(test_dir, dict_objs)
        tests.assert_FS(test_dir, dict_objs)

    # ===========================================
    # Equality between setup_FS() and assert_FS()
    # ===========================================
    test_eq({})
    test_eq({'file': 0})
    test_eq({'file': 10})
    test_eq({'dir': {}})
    test_eq({
        'dir1': {},
        'file1': 123,
        'dir2': {
            'file2': 321,
        },
    })
    test_eq({
        'dir1': {},
        'file1': 123,
        'dir2': {
            'file2': 321,
            'file3': 321,
            'dir3': {
                'file4': 1,
            },
        },
    })


if __name__ == '__main__':
    t = tempfile.TemporaryDirectory()
    test_assert_FS(t.name)

    t = tempfile.TemporaryDirectory()
    test_setup_FS_assert_FS(t.name)
