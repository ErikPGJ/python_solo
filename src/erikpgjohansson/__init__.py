# NOTE: This command is needed for this package to be treated as a "namespace
# package" (i.e. the directory is present in both this distribution package and
# another and both combine to form one package).
__path__ = __import__('pkgutil').extend_path(__path__, __name__)

# NOTE: This file MUST NOT CONTAIN ANY ADDITIONAL CODE (besides documentation),
# due to being in a namespace package.
