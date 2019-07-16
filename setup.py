import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mongo-sio",
    version="0.0.1",
    author="Sam Partridge",
    description="Sans-io implementation of the MongoDB protocol",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SamP20/mongo-sio",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["bson"]
)