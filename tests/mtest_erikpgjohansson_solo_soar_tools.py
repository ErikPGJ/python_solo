'''
*Manual* test code for
erikpgjohansson.solo.soar.tools.

It is useful to *not* do this automatically with other tests since it
downloads data from SOAR (over the internet) which in turn:
* slows down tests,
* may increase the risk of having one's IP blacklisted by SOAR.
'''


import erikpgjohansson.solo.soar.tools
import tempfile


def mtest_download_zip_JSON_SDTs(tmp_path):
    # See if crashes.
    erikpgjohansson.solo.soar.tools.download_zip_JSON_SDTs(tmp_path)


if __name__ == '__main__':
    if 1:
        t = tempfile.TemporaryDirectory()
        mtest_download_zip_JSON_SDTs(t.name)
