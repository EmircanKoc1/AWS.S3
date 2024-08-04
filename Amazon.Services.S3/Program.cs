using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using s3 = Amazon.S3.Util;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();


builder.Services.AddDefaultAWSOptions(builder.Configuration.GetAWSOptions());
builder.Services.AddAWSService<IAmazonS3>();




var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();


app.MapGet("/list-buckets", async (
    [FromServices] IAmazonS3 _amazonS3) =>
{
    return Results.Ok((await _amazonS3.ListBucketsAsync()).Buckets);
});

app.MapPost("/create-bucket", async (
    [FromQuery] string bucketName,
    [FromServices] IAmazonS3 _amazonS3) =>
{
    if (await s3.AmazonS3Util.DoesS3BucketExistV2Async(_amazonS3, bucketName))
        return "bucket already exists";

    var response = await _amazonS3.PutBucketAsync(bucketName);

    if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
        return "progress succesfull";

    return "progress failed";
});


app.MapDelete("/delete-bucket/{bucketName}", async (
    [FromRoute] string bucketName,
    [FromServices] IAmazonS3 _amazonS3) =>
{
    var response = await _amazonS3.DeleteBucketAsync(bucketName);

    if (response.HttpStatusCode is System.Net.HttpStatusCode.OK)
        return "progress succesfull";

    return "progress failed";

});


app.MapPost("/upload-file/{bucketName}", async (
    [FromForm] IFormFile file,
    [FromServices] IAmazonS3 _amazonS3,
    [FromRoute] string bucketName,
    [FromQuery] string? folderName) =>
{
    if (!await _amazonS3.DoesS3BucketExistAsync(bucketName))
        return Results.NotFound("bucket not exists");

    var requestObject = new PutObjectRequest()
    {
        BucketName = bucketName,
        Key = $"{(!string.IsNullOrWhiteSpace(folderName) ? string.Concat(folderName, "/") : string.Empty)}{file.FileName}",
        InputStream = file.OpenReadStream()
    };

    requestObject.Metadata.Add("Content-Type", file.ContentType);

    var response = await _amazonS3.PutObjectAsync(requestObject);

    if (response.HttpStatusCode is System.Net.HttpStatusCode.OK)
        return Results.Ok("progress successfull");

    return Results.NotFound("progress failed");

}).DisableAntiforgery();


app.MapGet("/get-files/{bucketName}", async (
    [FromServices] IAmazonS3 _amazonS3,
    [FromRoute] string bucketName,
    [FromQuery] string? folderName) =>
{

    if (!await _amazonS3.DoesS3BucketExistAsync(bucketName))
        return Results.NotFound("bucket not found !");

    ListObjectsV2Request listRequest = new ListObjectsV2Request()
    {
        BucketName = bucketName,
        Prefix = folderName
    };
    ListObjectsV2Response response = await _amazonS3.ListObjectsV2Async(listRequest);
    if (response.HttpStatusCode is HttpStatusCode.OK)
    {
        ICollection<Task<string>> tasks = new List<Task<string>>();

        var taskDtos = response.S3Objects.Select(s3Object =>
         {

             GetPreSignedUrlRequest urlRequest = new GetPreSignedUrlRequest()
             {
                 BucketName = bucketName,
                 Key = s3Object.Key,
                 Expires = DateTime.UtcNow.AddMinutes(1)
             };

             var task = _amazonS3.GetPreSignedURLAsync(urlRequest);
             tasks.Add(task);

             return new S3ObjectTaskDTO(s3Object.Key, task);
         });

        await Task.WhenAll(tasks);

        return Results.Ok(taskDtos.Select(S3TaskObjectDTO => new S3ObjectDTO(
            name: S3TaskObjectDTO.name,
            url: S3TaskObjectDTO.task.Result)));
    }
    return Results.NotFound("bucket is empty");

});


app.MapDelete("/delete-file/{bucketName}/{fileName}", async (
    [FromRoute] string bucketName,
    [FromRoute] string fileName,
    [FromServices] IAmazonS3 _amazonS3) =>
{
    if (!await _amazonS3.DoesS3BucketExistAsync(bucketName))
        return Results.NotFound("Bucket does not exists");

    var deleteResponse = await _amazonS3.DeleteObjectAsync(bucketName, fileName);

    if (deleteResponse.HttpStatusCode is HttpStatusCode.NoContent)
        return Results.Ok("bucket was deleted");

    return Results.BadRequest();

});


app.MapGet("/download-file-by-name/{bucketName}/{fileName}", async (
    [FromRoute] string bucketName,
    [FromRoute] string fileName,
    [FromServices] IAmazonS3 _amazonS3) =>
{
    GetObjectResponse response = await _amazonS3.GetObjectAsync(bucketName, fileName);
    return Results.File(response.ResponseStream, response.Headers.ContentType);
});



app.Run();

public record S3ObjectDTO(string name, string url);
public record S3ObjectTaskDTO(string name, Task<string> task);
