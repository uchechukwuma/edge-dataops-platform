from setuptools import setup, Extension
import sys

# Compiler flags for optimization
if sys.platform == 'win32':
    extra_flags = ['/O2', '/W3']
else:
    extra_flags = ['-O3', '-Wall']

module = Extension(
    'validator',
    sources=['validator.c'],
    extra_compile_args=extra_flags,
)

setup(
    name='validator',
    version='1.0',
    description='C extension for high-speed sensor validation',
    ext_modules=[module],
)
