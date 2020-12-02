import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="my_fits_datalake",
    version="0.0.1",

    description="An CDK Python app for building a FITS data lake",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "my_fits_datalake"},
    packages=setuptools.find_packages(where="my_fits_datalake"),

    install_requires=[
        "aws-cdk.core==1.49.1",
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
