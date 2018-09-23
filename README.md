# IXIAN Project

## About IXIAN

Ixian DLT is a revolutionary blockchain that brings several innovative advantages, such as processing a high volume of micro-transactions quickly while consuming a low amount of processing power, disk space and energy. 

**Homepage**: [IXIAN Project Homepage](https://www.ixian.io "IXIAN")

## The repository

The IXIAN  repository is divided into three main projects:

* IxianCore: Functionality common to both DLT and S2
* IxianDLT: Implementation of the blockchain-processing part of the project.
* IxianS2: Implementation of the streaming network.

## Development branches

There are two main development branches:
* master-mainnet: This branch is used to build the binaries for the official IXIAN DLT network. It should change slowly and be quite well-tested.
* master: This is the main development branch and the source for testnet binaries. The branch might not always be kept bug-free, if an extensive new feature is being worked on. If you are simply looking to build a current testnet binary yourself, please use one of the release tags which will be associated with the master branch.

## Building

Visual Studio 2017 is required (Community Edition is fine), you can get it from here: [Visual Studio](https://visualstudio.microsoft.com/)

Several NuGetPackages are downloaded automatically during the build process.

## DLT Hybrid PoW Miner

The mining section of the code expects an Argon2 DLL in the DLL search path. You can build one using this project: [Argon2 Reference implementation](https://github.com/P-H-C/phc-winner-argon2)

## Get in touch / Contributing

If you feel like you can contribute to the project, or have questions or comments, you can get in touch with the team through Discord: (https://discord.gg/dbg9WtR)

## Pull requests

If you would like to send an improvement or bugfix to this repository, but without permanently joining the team, follow these approximate steps:

1. Fork this repository
2. Create a branch (preferably with a name that describes the change)
3. Create commits (the commit messages should contain some information on what and why was changed)
4. Create a pull request to this repository for review and inclusion.
