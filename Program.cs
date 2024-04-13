using MessageBroker.Data;
using MessageBroker.Models;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(opt => opt.UseSqlite("Data Source=MessageBroker.db"));

var app = builder.Build();

app.UseHttpsRedirection();

// Endpoints
// Create - Post request
app.MapPost("api/topics", async (AppDbContext context, Topic topic) =>
{
    await context.Topics.AddAsync(topic);

    await context.SaveChangesAsync();

    return Results.Created($"api/topics/{topic.Id}", topic);
});

// Return all topics - Get request
app.MapGet("api/topics", async (AppDbContext context) =>
{
    var topics = await context.Topics.ToListAsync();

    return Results.Ok(topics);
});

// Publish Message 
app.MapPost("api/topics/{id}/messages", async (AppDbContext context, int id, Message message) =>
{
    // Return true if there is any topic
    bool topics = await context.Topics.AnyAsync(t => t.Id == id);

    //  if false
    if (!topics)
    {
        return Results.NotFound("Topic not found");
    }

    var subs = context.Subscriptions.Where(s => s.TopicId == id);

    // If no subscriptions
    if (subs.Count() == 0)
    {
        return Results.NotFound("There are not subscriptions for this topic");
    }

    // Add message for the broker - foreach sub in collection of subs
    foreach (var sub in subs)
    {
        Message msg = new Message
        {
            TopicMessage = message.TopicMessage,
            SubscriptionId = sub.Id,
            ExpiresAfter = message.ExpiresAfter,
            MessageStatus = message.MessageStatus,
        };

        // Add message to the database
        await context.Messages.AddAsync(msg);
    }
    // Save the changes to the database
    await context.SaveChangesAsync();

    // Returns the result 
    return Results.Ok("Message has been published!");
});


// Create subscription
app.MapPost("api/topics/{id}/subscriptions", async (AppDbContext context, int id, Subscription sub) =>
{
    // Not adding a subscription that doest not exist
    // Return true if topic id exists
    bool topics = await context.Topics.AnyAsync(t => t.Id == id);

    //  if false
    if (!topics)
    {
        return Results.NotFound("Subscription not found");
    }

    // Check was already made for the topic id
    sub.TopicId = id;

    await context.Subscriptions.AddAsync(sub);
    await context.SaveChangesAsync();

    return Results.Created($"api/topics/{id}/subscriptions/{sub.Id}", sub);

});


// Get Subscriber messages
app.MapGet("api/subscriptions/{id}/messages", async (AppDbContext context, int id) =>
{
    // Body of the endpoint 
    // Check if subscription id is correct
    bool subs = await context.Subscriptions.AnyAsync(s => s.Id == id);
    if (!subs)
    {
        return Results.NotFound("Subcription not found!");
    }

    // If any messages
    var messages = context.Messages.Where(m => m.SubscriptionId == id && m.MessageStatus != "SENT");

    if (messages.Count() == 0)
    {
        return Results.NotFound("No new message");
    }

    // 
    foreach (var msg in messages)
    {
        msg.MessageStatus = "REQUESTED";
    }

    // Save to database
    await context.SaveChangesAsync();
    return Results.Ok(messages);

});


// Acknowledge messages for Subscriber
// Passing array of int of the messages
app.MapPost("api/subscriptions/{id}/messages", async (AppDbContext context, int id, int[] confirmation) =>
{
    // Check if subscription id is correct
    bool subs = await context.Subscriptions.AnyAsync(s => s.Id == id);
    if (!subs)
    {
        return Results.NotFound("Subcription not found!");
    }

    // Validation if any confirmations
    if (confirmation.Length <= 0)
    {
        return Results.BadRequest();
    }

    int counter = 0;

    foreach (var i in confirmation)
    {
        var msg = context.Messages.FirstOrDefault(m => m.Id == i);

        if (msg is not null)
        {
            msg.MessageStatus = "SENT";
            await context.SaveChangesAsync();
            // Increment counter after save changes in case where message is not null
            counter++;
        }
    }

    return Results.Ok($"Acknowledged {counter}/{confirmation.Length} messages");
});

app.Run();

