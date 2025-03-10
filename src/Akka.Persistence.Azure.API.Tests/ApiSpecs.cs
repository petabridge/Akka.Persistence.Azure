using Akka.Persistence.Azure.Hosting;
using static PublicApiGenerator.ApiGenerator;

namespace Akka.Persistence.Azure.API.Tests;

public class ApiSpecs
{
    private static Task VerifyAssembly<T>()
    {
        return Verify(GeneratePublicApi(typeof(T).Assembly));
    }

    [Fact]
    public Task ApproveCore() => VerifyAssembly<AzurePersistence>();
    
    [Fact]
    public Task ApproveHosting() => VerifyAssembly<AzureBlobSnapshotOptions>();
}