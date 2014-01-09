from distutils.core import setup


setup(name='Zorro',
      version='0.1.4',
      description='Network communication library, with zeromq support',
      author='Paul Colomiets',
      author_email='paul@colomiets.name',
      url='http://github.com/tailhook/zorro',
      classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        ],
      packages=['zorro', 'zorro.mongodb'],
    )
