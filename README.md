# Akka.Persistence.Azure

Akka.Persistence implementation that uses Windows Azure table and blob storage.

## Configuration

Here is a default configuration used by this plugin: https://github.com/petabridge/Akka.Persistence.Azure/blob/dev/src/Akka.Persistence.Azure/reference.conf

You will need to provide connection string and Azure Table name for journal, and connection string with container name for Azure Blob Store:
```
# Need to enable plugin
akka.persistence.journal.plugin = akka.persistence.journal.azure-table
akka.persistence.snapshot-store.plugin = akka.persistence.snapshot-store.azure-blob-store

# Configure journal
akka.persistence.journal.azure-table.connection-string = "Your Azure Storage connection string"
akka.persistence.journal.azure-table.table-name = "Your table name"

# Configure snapshots
akka.persistence.snapshot-store.azure-blob-store.connection-string = "Your Azure Storage connection string"
akka.persistence.snapshot-store.azure-blob-store.container-name = "Your container name"
```

### Local development mode
You can turn local development mode by changing these two settings:
```
akka.persistence.journal.azure-table.development = on
akka.persistence.snapshot-store.azure-blob-store.development = on
```
When set, the plugin will ignore the `connection-string` setting and uses the Azure Storage Emulator default connection string of "UseDevelopmentStorage=true" instead.

### Configuring snapshots Blob Storage

#### Auto-initialize blob container

Blob container auto-initialize behaviour can be changed by changing this flag setting:
```
# Creates the required container if set
akka.persistence.snapshot-store.azure-blob-store.auto-initialize = on
```

#### Container public access type

Auto-initialized blob container public access type can be controlled by changing this setting:
```
# Public access level for the auto-initialized storage container. 
# Valid values are "None", "BlobContainer" or "Blob"
akka.persistence.snapshot-store.azure-blob-store.container-public-access-type = "None"
```

#### DefaultAzureCredential

`Azure.Identity` `DefaultAzureCredential` can be used to configure the resource by using `AzureBlobSnapshotSetup`. 
When using `DefaultAzureCredential`, the HOCON 'connection-string' setting is ignored.

Example:
```
var blobStorageSetup = AzureBlobSnapshotSetup.Create(
  new Uri("https://{account_name}.blob.core.windows.net"), // This is the blob service URI
  new DefaultAzureCredential() // You can pass a DefaultAzureCredentialOption here.
                               // https://docs.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet
);

var bootstrap = BootstrapSetup.Create().And(blobStorageSetup);
var system = ActorSystem.Create("actorSystem", bootstrap);
```

## Using the plugin in local development environment

You can use this plugin with Azure Storage Emulator in a local development environment by setting the development flag in the configuration file:
```
akka.persistence.journal.azure-table.development = on
akka.persistence.snapshot-store.azure-blob-store.development = on
```

you do **not** need to provide a connection string for this to work, it is handled automatically by the Microsoft Azure SDK.

## Building this solution
To run the build script associated with this solution, execute the following:

**Windows**
```
c:\> build.cmd all
```

**Linux / OS X**
```
c:\> build.sh all
```

If you need any information on the supported commands, please execute the `build.[cmd|sh] help` command.

This build script is powered by [FAKE](https://fake.build/); please see their API documentation should you need to make any changes to the [`build.fsx`](build.fsx) file.

### Conventions
The attached build script will automatically do the following based on the conventions of the project names added to this project:

* Any project name ending with `.Tests` will automatically be treated as a [XUnit2](https://xunit.github.io/) project and will be included during the test stages of this build script;
* Any project name ending with `.Tests` will automatically be treated as a [NBench](https://github.com/petabridge/NBench) project and will be included during the test stages of this build script; and
* Any project meeting neither of these conventions will be treated as a NuGet packaging target and its `.nupkg` file will automatically be placed in the `bin\nuget` folder upon running the `build.[cmd|sh] all` command.

### DocFx for Documentation
This solution also supports [DocFx](http://dotnet.github.io/docfx/) for generating both API documentation and articles to describe the behavior, output, and usages of your project. 

All of the relevant articles you wish to write should be added to the `/docs/articles/` folder and any API documentation you might need will also appear there.

All of the documentation will be statically generated and the output will be placed in the `/docs/_site/` folder. 

#### Previewing Documentation
To preview the documentation for this project, execute the following command at the root of this folder:

```
C:\> serve-docs.cmd
```

This will use the built-in `docfx.console` binary that is installed as part of the NuGet restore process from executing any of the usual `build.cmd` or `build.sh` steps to preview the fully-rendered documentation. For best results, do this immediately after calling `build.cmd buildRelease`.

### Release Notes, Version Numbers, Etc
This project will automatically populate its release notes in all of its modules via the entries written inside [`RELEASE_NOTES.md`](RELEASE_NOTES.md) and will automatically update the versions of all assemblies and NuGet packages via the metadata included inside [`common.props`](src/common.props).

If you add any new projects to the solution created with this template, be sure to add the following line to each one of them in order to ensure that you can take advantage of `common.props` for standardization purposes:

```
<Import Project="..\common.props" />
```
