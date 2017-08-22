from socrata import Socrata
from socrata.authorization import Authorization
from socrata.revisions import Revisions, Revision
from socrata.sources import Sources, Source
from socrata.input_schema import InputSchema
from socrata.output_schema import OutputSchema
from socrata.job import Job
from socrata.configs import Configs, Config
import inspect
import sys
import re

classes = [
  Socrata,
  Authorization,
  Revisions, Revision,
  Sources, Source,
  Configs, Config,
  InputSchema,
  OutputSchema,
  Job
]

def arg_spec_str(thing):
    return '`%s`' % str(inspect.getargspec(thing))

def class_lines(klass):
    return [
        '',
        '## %s' % klass.__name__,
        arg_spec_str(klass),
        '',
        inspect.getdoc(klass) or 'DocumentThis!'
    ]


def func_lines(funcname, func):
    return [
        '',
        '### %s' % funcname,
        arg_spec_str(func),
        '',
        inspect.getdoc(func) or 'DocumentThis!'
    ]


lines = ['<!-- doc -->', '# Library Docs']

for klass in classes:
    lines += class_lines(klass)

    funcs = inspect.getmembers(klass, predicate=inspect.isfunction)

    for (funcname, func) in funcs:
        if not funcname.startswith('_'):
            lines += func_lines(funcname, func)

lines.append('<!-- docstop -->')


with open('README.md', 'r') as f:
    readme = f.read()
    docs = '\n'.join(lines)
    p = re.compile('<!-- doc -->.*<!-- docstop -->', re.DOTALL)
    updated_readme = p.sub(docs, readme)

with open('README.md', 'w') as f:
    f.write(updated_readme)


