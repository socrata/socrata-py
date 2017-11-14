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
    spec = inspect.getargspec(thing)
    args = spec.args

    # Hacks for closing over self and uri
    if len(args) and args[0] == 'self':
        args = args[1:]
    if len(args) and args[0] == 'uri':
        args = args[1:]

    if not len(args):
        return ''

    arg_str = '\n    Args: ' + ', '.join(args)

    default_str = ''
    if spec.defaults and any([d != None for d in spec.defaults]):
        defaults = list(spec.defaults)
        default_str = '\n    Defaults: ' + ', '.join([
            name + '=' + str(default)
            for (name, default) in zip(args, defaults) if default != None
        ])

    return '```\nArgSpec{arg_str}{default_str}\n```'.format(
        arg_str = arg_str,
        default_str = default_str
    )

def link_to(thing):
    [_, rel] = inspect.getsourcefile(thing).split('socrata-py')
    (_lines, line_num) = inspect.getsourcelines(thing)

    return 'https://github.com/socrata/socrata-py/blob/master/{rel}#L{line_num}'.format(
        rel = rel,
        line_num = line_num
    )

def format_doc(doc):
    return doc



def class_lines(klass):
    doc = inspect.getdoc(klass)
    return [
        '',
        '### [%s](%s)' % (klass.__name__, link_to(klass)),
        arg_spec_str(klass),
        '',
        format_doc(doc or '')
    ]

def func_lines(funcname, func):
    doc = inspect.getdoc(func)
    if doc:
        return [
            '',
            '#### [%s](%s)' % (funcname, link_to(func)),
            arg_spec_str(func),
            '',
            format_doc(doc)
        ]
    return []


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


