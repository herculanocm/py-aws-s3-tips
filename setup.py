import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="de_aws_s3_tips",
    version="0.0.2",
    author="Herculano Cunha",
    author_email="herculanocm@outlook.com",
    description="Data Engineer tips for AWS S3",
    download_url='https://github.com/herculanocm/py-aws-s3-tips/archive/master.zip',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
     keywords='AWS Python boto3 S3 tools tips',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
    install_requires=['boto3','zip-files'],
)