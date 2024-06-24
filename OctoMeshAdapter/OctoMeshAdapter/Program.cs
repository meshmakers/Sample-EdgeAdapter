using System.Timers;
using Meshmakers.Octo.Sdk.Common.Adapters;
using Meshmakers.Octo.Sdk.Common.EtlDataPipeline;
using Meshmakers.Octo.Sdk.Common.EtlDataPipeline.Configuration;
using Meshmakers.Octo.Sdk.Common.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// Set the configuration for the adapter.
// In a real production environment this properties would be set externaly as environment variables.
// For more configuration options see Meshmakers.Octo.Sdk.Common.Adapters.AdapterOptions


Environment.SetEnvironmentVariable("OCTO_ADAPTER__AdapterRtId", "66793eb0d56225bac531bd04");
Environment.SetEnvironmentVariable("OCTO_ADAPTER__TENANTID", "adaptertest");


var b = new AdapterBuilder();
b.Run([], (context, services) =>
{
    services.AddDataPipeline()
        .RegisterNode<MyTestPipeline>()
        .RegisterEtlContext<IAdapterEtlContext>();
    
    services.AddSingleton<IAdapterService, MyFirstAdapterService>();
    
});




public class MyFirstAdapterService(ILogger<MyFirstAdapterService> logger, 
    IPipelineExecutionService pipelineExecutionService) : IAdapterService
{
    private System.Timers.Timer? _timer;
    public Task StartupAsync(AdapterStartup adapterStartup, CancellationToken stoppingToken)
    {
        logger.LogInformation("Hello world from the adapter");

        // this is a plain string from the adapter configuration and can be configured in the AdminUI.
        // it would make sense to use a json object here but anything is possible.
        var config = adapterStartup.Configuration.AdapterConfiguration;

        foreach (var pipelineconfig in adapterStartup.Configuration.Pipelines)
        {
            // let the pipeline execution service know about the pipeline
            pipelineExecutionService.RegisterPipeline(adapterStartup.TenantId, pipelineconfig);
        }
        
        
        // and now we have to create a trigger when the pipeline should be executed. 
        // this can be done by a timer, a message from a 3rd party api or anything else.

        _timer = new(10_000);
        
        // data can be whatever you want to pass to the pipeline.
        var data = new Dictionary<string, object>();
        
        _timer.Elapsed += (sender, args) =>
        {
            pipelineExecutionService.ExecuteAllPipelinesAsync(new ExecutePipelineOptions(DateTime.Now), data);
        };
        _timer.Start();


        return Task.CompletedTask;
    }



    public Task ShutdownAsync(AdapterShutdown adapterShutdown, CancellationToken stoppingToken)
    {
        logger.LogInformation("Goodbye world from the adapter");
        
        // this part will be called when the adapter is either stopped or receives an updated configuration.

        // cleanup
        pipelineExecutionService.UnregisterAllPipelines(adapterShutdown.TenantId);
        _timer!.Stop();
        _timer = null;
        
        return Task.CompletedTask;
    }
}


[NodeConfiguration(typeof(MyNodeConfiguration))]
public class MyTestPipeline(NodeDelegate next) : IPipelineNode
{
    public async Task ProcessObjectAsync(IDataContext dataContext)
    {
        dataContext.Logger.Info(dataContext.NodeStack.Peek(), "Hello world from the pipeline");
        await next(dataContext);
    }
}


[NodeName("MyPipeline", 1)]
public class MyNodeConfiguration : NodeConfiguration;