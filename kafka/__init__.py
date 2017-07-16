try:  # pragma: no cover
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:  # pragma: no cover
    from pkgutil import extend_path
    __path__ = extend_path(__path__, __name__)
