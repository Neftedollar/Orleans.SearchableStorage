using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

internal sealed class WriteFaultInjectingGrainStorage : IGrainStorage
{
    private readonly IGrainFactory _grainFactory;
    private readonly IGrainStorage _inner;

    public WriteFaultInjectingGrainStorage(IGrainStorage inner, IGrainFactory grainFactory)
    {
        _inner = inner;
        _grainFactory = grainFactory;
    }

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        return _inner.ReadStateAsync(stateName, grainId, grainState);
    }

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        await GetFaultGrain(PhysicalWriteFaultStage.BeforeCommit, stateName).OnWrite(grainId);
        await _inner.WriteStateAsync(stateName, grainId, grainState);
        await GetFaultGrain(PhysicalWriteFaultStage.AfterCommit, stateName).OnWrite(grainId);
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        return _inner.ClearStateAsync(stateName, grainId, grainState);
    }

    public static string CreateFaultGrainKey(PhysicalWriteFaultStage stage, string stateName)
    {
        return $"{stage}:{stateName}";
    }

    private IStorageFaultGrain GetFaultGrain(PhysicalWriteFaultStage stage, string stateName)
    {
        return _grainFactory.GetGrain<IStorageFaultGrain>(CreateFaultGrainKey(stage, stateName));
    }
}

internal enum PhysicalWriteFaultStage
{
    BeforeCommit = 0,
    AfterCommit = 1,
}
