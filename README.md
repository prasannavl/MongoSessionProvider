MongoSessionProvider
====================

ASP.NET Session Provider for MongoDB

### Status Note

> **Not maintained since: 2014**

> This project is not maintained. A lot has changed since then, and ASP.NET Core provides much better designed paradigms. That being said, it should work fine if you need to support some legacy code. 

Installation:

```
PM> Install-Package PVL.MongoSessionProvider
```

NuGet Pacakge Link: https://nuget.org/packages/PVL.MongoSessionProvider

Details: https://www.prasannavl.com/2013/02/mongodb-session-state-provider-for.html

Example session document:

    > use SessionState;
    > db.Sessions.find().pretty(); 

    {
            "_id" : "i2guetwsm0mgaibb1gqmodfq",
            "App" : "/",
            "Created" : ISODate("2013-02-21T22:27:32.091Z"),
            "Expires" : ISODate("2013-02-22T22:30:59.267Z"),
            "LockDate" : ISODate("2013-02-21T22:29:54.481Z"),
            "LockId" : 1,
            "Timeout" : 20,
            "Locked" : true,
            "Items" : "AQAAAP////8EVGVzdAgAAAABBkFkcmlhbg==",
            "Flags" : 0
         }

   

Scheduled session cleanup command:
db.Sessions.remove({"Expires" : {$lt : new Date() }})
   
Example web.config settings:

    ..
    <connectionStrings>
       <add name="SessionState" connectionString="mongodb://localhost"/>
    </connectionStrings>
    <system.web>
       <sessionState mode="Custom" timeout="1440" cookieless="false" customProvider="MongoSessionStateProvider">
         <providers>
           <add name="MongoSessionStateProvider" type="PVL.MongoSessionProvider" connectionStringName="SessionState" writeExceptionsToEventLog="false"/>
         </providers>
       </sessionState>
    </system.web>
    .. 
