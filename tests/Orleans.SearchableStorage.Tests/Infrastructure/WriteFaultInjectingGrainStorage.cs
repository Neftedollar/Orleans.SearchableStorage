using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.SearchableStorage.Tests.Infrastructure;

internal sealed class WriteFaultInjectingGrainStorage : IGrainStorage
{
    internal const string InjectedFailureMessage = "Injected physical persistence failure.";

    private const string FaultGrainKey = "Orleans.SearchableStorage.Tests.PhysicalStorageFaults";
    private readonly IGrainFactory _grainFactory;
    private readonly IGrainStorage _inner;

    public WriteFaultInjectingGrainStorage(IGrainStorage inner, IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(grainFactory);
        _inner = inner;
        _grainFactory = grainFactory;
    }

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        return _inner.ReadStateAsync(stateName, grainId, grainState);
    }

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var faults = GetFaultGrain();
        await faults.OnBeforeAsync(grainId, stateName, PhysicalStorageOperation.Write);
        await _inner.WriteStateAsync(stateName, grainId, grainState);
        await faults.OnAfterAsync(grainId, stateName, PhysicalStorageOperation.Write);
    }

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var faults = GetFaultGrain();
        await faults.OnBeforeAsync(grainId, stateName, PhysicalStorageOperation.Clear);
        await _inner.ClearStateAsync(stateName, grainId, grainState);
        await faults.OnAfterAsync(grainId, stateName, PhysicalStorageOperation.Clear);
    }

    public static Task AddWriteFaultAsync(
        IGrainFactory grainFactory,
        GrainId grainId,
        string stateName,
        PhysicalWriteFaultStage stage,
        int call = 1)
    {
        return AddFaultAsync(
            grainFactory,
            grainId,
            stateName,
            PhysicalStorageOperation.Write,
            stage,
            call);
    }

    public static Task AddClearFaultAsync(
        IGrainFactory grainFactory,
        GrainId grainId,
        string stateName,
        PhysicalWriteFaultStage stage,
        int call = 1)
    {
        return AddFaultAsync(
            grainFactory,
            grainId,
            stateName,
            PhysicalStorageOperation.Clear,
            stage,
            call);
    }

    public static Task<int> GetWriteCallCountAsync(
        IGrainFactory grainFactory,
        GrainId grainId,
        string stateName)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        return grainFactory.GetGrain<IPhysicalStorageFaultGrain>(FaultGrainKey).GetCallCountAsync(
            grainId,
            stateName,
            PhysicalStorageOperation.Write);
    }

    private static Task AddFaultAsync(
        IGrainFactory grainFactory,
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation,
        PhysicalWriteFaultStage stage,
        int call)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(call);
        return grainFactory.GetGrain<IPhysicalStorageFaultGrain>(FaultGrainKey).ScheduleAsync(
            grainId,
            stateName,
            operation,
            stage,
            call,
            InjectedFailureMessage);
    }

    private IPhysicalStorageFaultGrain GetFaultGrain()
    {
        return _grainFactory.GetGrain<IPhysicalStorageFaultGrain>(FaultGrainKey);
    }
}

internal interface IPhysicalStorageFaultGrain : IGrainWithStringKey
{
    Task ScheduleAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation,
        PhysicalWriteFaultStage stage,
        int call,
        string errorMessage);

    Task OnBeforeAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation);

    Task OnAfterAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation);

    Task<int> GetCallCountAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation);
}

internal sealed class PhysicalStorageFaultGrain : Grain, IPhysicalStorageFaultGrain
{
    private readonly Dictionary<FaultTarget, int> _callCounts = [];
    private readonly List<ScheduledFault> _faults = [];

    public Task ScheduleAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation,
        PhysicalWriteFaultStage stage,
        int call,
        string errorMessage)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(call);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorMessage);
        var target = new FaultTarget(grainId, stateName, operation);
        var currentCall = _callCounts.GetValueOrDefault(target);
        _faults.Add(new ScheduledFault(
            target,
            checked(currentCall + call),
            stage,
            errorMessage));
        DelayDeactivation(TimeSpan.FromMinutes(5));
        return Task.CompletedTask;
    }

    public Task OnBeforeAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation)
    {
        var target = new FaultTarget(grainId, stateName, operation);
        var call = checked(_callCounts.GetValueOrDefault(target) + 1);
        _callCounts[target] = call;
        ThrowIfScheduled(target, call, PhysicalWriteFaultStage.BeforeCommit);
        return Task.CompletedTask;
    }

    public Task OnAfterAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation)
    {
        var target = new FaultTarget(grainId, stateName, operation);
        ThrowIfScheduled(
            target,
            _callCounts.GetValueOrDefault(target),
            PhysicalWriteFaultStage.AfterCommit);
        return Task.CompletedTask;
    }

    public Task<int> GetCallCountAsync(
        GrainId grainId,
        string stateName,
        PhysicalStorageOperation operation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stateName);
        var target = new FaultTarget(grainId, stateName, operation);
        return Task.FromResult(_callCounts.GetValueOrDefault(target));
    }

    private void ThrowIfScheduled(
        FaultTarget target,
        int call,
        PhysicalWriteFaultStage stage)
    {
        var index = _faults.FindIndex(
            fault => fault.Target == target
                && fault.Call == call
                && fault.Stage == stage);
        if (index < 0)
        {
            return;
        }

        var fault = _faults[index];
        _faults.RemoveAt(index);
        throw new InvalidOperationException(fault.ErrorMessage);
    }

    private readonly record struct FaultTarget(
        GrainId GrainId,
        string StateName,
        PhysicalStorageOperation Operation);

    private sealed record ScheduledFault(
        FaultTarget Target,
        int Call,
        PhysicalWriteFaultStage Stage,
        string ErrorMessage);
}

internal enum PhysicalStorageOperation
{
    Write = 0,
    Clear = 1,
}

public enum PhysicalWriteFaultStage
{
    BeforeCommit = 0,
    AfterCommit = 1,
}
