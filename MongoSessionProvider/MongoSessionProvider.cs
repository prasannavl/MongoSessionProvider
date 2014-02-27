// Author: Prasanna V. Loganathar
// Project: MongoSessionProvider
// Copyright (c) Launchark Technologies. All rights reserved.
// See License.txt in the project root for license information.
// 
// Created: 5:26 AM 27-02-2014

using System;
using System.Configuration;
using System.Configuration.Provider;
using System.Diagnostics;
using System.IO;
using System.Web;
using System.Web.Configuration;
using System.Web.SessionState;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace PVL
{
    /// <summary>
    ///     Custom ASP.NET Session State Provider using MongoDB as the state store.
    ///     Session state default store:
    ///     "Sessions" collection within a "SessionState" database.
    ///     Example session document:
    ///     {
    ///     "_id" : "i2guetwsm0mgaibb1gqmodfq",
    ///     "App" : "/",
    ///     "Created" : ISODate("2013-02-21T22:27:32.091Z"),
    ///     "Expires" : ISODate("2013-02-22T22:30:59.267Z"),
    ///     "LockDate" : ISODate("2013-02-21T22:29:54.481Z"),
    ///     "LockId" : 1,
    ///     "Timeout" : 20,
    ///     "Locked" : true,
    ///     "Items" : "AQAAAP////8EVGVzdAgAAAABBkFkcmlhbg==",
    ///     "Flags" : 0
    ///     }
    ///     Scheduled session cleanup:
    ///     db.Sessions.remove({"Expires" : {$lt : new Date() }})
    ///     Example web.config settings:
    ///     ..
    ///     <connectionStrings>
    ///         <add name="SessionState" connectionString="mongodb://localhost" />
    ///     </connectionStrings>
    ///     <system.web>
    ///         <sessionState mode="Custom" timeout="1440" cookieless="false" customProvider="MongoSessionStateProvider">
    ///             <providers>
    ///                 <add name="MongoSessionStateProvider" type="PVL.MongoSessionProvider"
    ///                     connectionStringName="SessionState" writeExceptionsToEventLog="false" />
    ///             </providers>
    ///         </sessionState>
    ///     </system.web>
    ///     ..
    /// </summary>
    public sealed class MongoSessionProvider : SessionStateStoreProviderBase
    {
        private const string ExceptionMessage = "An error occurred. Please contact support if the problem persists.";
        private const string EventSource = "MongoSessionStateStore";
        private const string EventLog = "Application";
        private string applicationName;
        private String collectionName;
        private SessionStateSection config;
        private string connectionString;
        private ConnectionStringSettings connectionStringSettings;
        private String databaseName;
        private bool writeExceptionsToEventLog;
        private WriteConcern writeMode;


        public string ApplicationName
        {
            get { return applicationName; }
        }


        public bool WriteExceptionsToEventLog
        {
            get { return writeExceptionsToEventLog; }
            set { writeExceptionsToEventLog = value; }
        }


        private MongoCollection<BsonDocument> GetSessionCollection()
        {
            var client = new MongoClient(connectionString);
            return client.GetServer().GetDatabase(databaseName).GetCollection(collectionName);
        }

        public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config)
        {
            // Initialize values from web.config.
            if (config == null)
                throw new ArgumentNullException("config");

            if (name == null || name.Length == 0)
                name = "MongoSessionStateStore";

            if (String.IsNullOrEmpty(config["description"]))
            {
                config.Remove("description");
                config.Add("description", "MongoDB Session State Store provider");
            }

            // Initialize the abstract base class.
            base.Initialize(name, config);

            // Initialize the ApplicationName property.
            applicationName = System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath;

            // Get <sessionState> configuration element.
            Configuration cfg = WebConfigurationManager.OpenWebConfiguration(ApplicationName);
            this.config = (SessionStateSection) cfg.GetSection("system.web/sessionState");

            // Initialize connection string.
            connectionStringSettings = ConfigurationManager.ConnectionStrings[config["connectionStringName"]];
            databaseName = config["databaseName"] ?? "SessionState";
            collectionName = config["collectionName"] ?? "Sessions";

            if (connectionStringSettings == null || connectionStringSettings.ConnectionString.Trim() == "")
            {
                throw new ProviderException("Connection string cannot be blank.");
            }

            connectionString = connectionStringSettings.ConnectionString;

            // Initialize WriteExceptionsToEventLog
            writeExceptionsToEventLog = false;

            if (config["writeExceptionsToEventLog"] != null)
            {
                if (config["writeExceptionsToEventLog"].ToUpper() == "TRUE")
                    writeExceptionsToEventLog = true;
            }

            writeMode = WriteConcern.Unacknowledged;

            if (config["writeConcern"] != null)
            {
                int result;
                if (int.TryParse(config["writeConcern"], out result))
                    if ((result > -2))
                        writeMode.W = result;
            }
        }

        public override SessionStateStoreData CreateNewStoreData(System.Web.HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                SessionStateUtility.GetSessionStaticObjects(context),
                timeout);
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        /// <summary>
        ///     Serialize is called by the SetAndReleaseItemExclusive method to
        ///     convert the SessionStateItemCollection into a Base64 string to
        ///     be stored in MongoDB.
        /// </summary>
        private string Serialize(SessionStateItemCollection items)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                if (items != null)
                    items.Serialize(writer);

                writer.Close();

                return Convert.ToBase64String(ms.ToArray());
            }
        }

        /// <summary>
        ///     SessionStateProviderBase.SetAndReleaseItemExclusive
        /// </summary>
        public override void SetAndReleaseItemExclusive(HttpContext context,
            string id,
            SessionStateStoreData item,
            object lockId,
            bool newItem)
        {
            // Serialize the SessionStateItemCollection as a string.
            var sessItems = Serialize((SessionStateItemCollection) item.Items);
            var cTime = DateTime.UtcNow;
            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                if (newItem)
                {
                    var doc = new BsonDocument()
                        .Add("_id", id)
                        .Add("App", ApplicationName)
                        .Add("Created", cTime)
                        .Add("Expires", cTime.AddMinutes(item.Timeout))
                        .Add("LockDate", cTime)
                        .Add("LockId", 0)
                        .Add("Timeout", item.Timeout)
                        .Add("Locked", false)
                        .Add("Items", sessItems)
                        .Add("Flags", 0);

                    sessionCollection.Save(doc, writeMode);
                }
                else
                {
                    var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName),
                        Query.EQ("LockId", (Int32) lockId));
                    var update = Update.Set("Expires", cTime.AddMinutes(item.Timeout));
                    update.Set("Items", sessItems);
                    update.Set("Locked", false);
                    sessionCollection.Update(query, update, writeMode);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "SetAndReleaseItemExclusive");
                    throw new ProviderException(ExceptionMessage);
                }
                throw;
            }
        }

        /// <summary>
        ///     SessionStateProviderBase.GetItem
        /// </summary>
        public override SessionStateStoreData GetItem(HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(false, context, id, out locked,
                out lockAge, out lockId, out actionFlags);
        }

        /// <summary>
        ///     SessionStateProviderBase.GetItemExclusive
        /// </summary>
        public override SessionStateStoreData GetItemExclusive(HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(true, context, id, out locked,
                out lockAge, out lockId, out actionFlags);
        }

        /// <summary>
        ///     GetSessionStoreItem is called by both the GetItem and
        ///     GetItemExclusive methods. GetSessionStoreItem retrieves the
        ///     session data from the data source. If the lockRecord parameter
        ///     is true (in the case of GetItemExclusive), then GetSessionStoreItem
        ///     locks the record and sets a new LockId and LockDate.
        /// </summary>
        private SessionStateStoreData GetSessionStoreItem(bool lockRecord,
            HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actionFlags)
        {
            // Initial values for return value and out parameters.
            SessionStateStoreData item = null;
            lockAge = TimeSpan.Zero;
            lockId = null;
            locked = false;
            actionFlags = 0;

            // DateTime to check if current session item is expired.
            // String to hold serialized SessionStateItemCollection.
            var serializedItems = "";
            // True if a record is found in the database.
            var foundRecord = false;
            // True if the returned session item is expired and needs to be deleted.
            var deleteData = false;
            // Timeout value from the data store.
            var timeout = 0;
            IMongoQuery query = null;
            var cTime = DateTime.UtcNow;
            try
            {
                MongoCollection sessionCollection = GetSessionCollection();

                // lockRecord is true when called from GetItemExclusive and
                // false when called from GetItem.
                // Obtain a lock if possible. Ignore the record if it is expired.
                if (lockRecord)
                {
                    query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName), Query.EQ("Locked", false),
                        Query.GT("Expires", cTime));
                    var update = Update.Set("Locked", true);
                    update.Set("LockDate", cTime);
                    var result = sessionCollection.Update(query, update);

                    if (result.DocumentsAffected == 0)
                    {
                        // No record was updated because the record was locked or not found.
                        locked = true;
                    }
                }

                // Retrieve the current session item information.
                query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
                var results = sessionCollection.FindOneAs<BsonDocument>(query);

                if (results != null)
                {
                    var expires = results["Expires"].AsDateTime;

                    if (expires < cTime)
                    {
                        // The record was expired. Mark it as not locked.
                        locked = false;
                        // The session was expired. Mark the data for deletion.
                        deleteData = true;
                    }
                    else
                        foundRecord = true;

                    serializedItems = results["Items"].AsString;
                    lockId = results["LockId"].AsInt32;
                    lockAge = cTime.Subtract(results["LockDate"].AsDateTime);
                    actionFlags = (SessionStateActions) results["Flags"].AsInt32;
                    timeout = results["Timeout"].AsInt32;
                }

                // If the returned session item is expired, 
                // delete the record from the data source.
                if (deleteData)
                {
                    query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
                    sessionCollection.Remove(query, writeMode);
                }

                // The record was not found. Ensure that locked is false.
                if (!foundRecord)
                    locked = false;

                // If the record was found and you obtained a lock, then set 
                // the lockId, clear the actionFlags,
                // and create the SessionStateStoreItem to return.
                if (foundRecord && !locked)
                {
                    lockId = (int) lockId + 1;

                    query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
                    var update = Update.Set("LockId", (int) lockId);
                    update.Set("Flags", 0);
                    sessionCollection.Update(query, update, writeMode);

                    // If the actionFlags parameter is not InitializeItem, 
                    // deserialize the stored SessionStateItemCollection.
                    if (actionFlags == SessionStateActions.InitializeItem)
                        item = CreateNewStoreData(context, (int) config.Timeout.TotalMinutes);
                    else
                        item = Deserialize(context, serializedItems, timeout);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "GetSessionStoreItem");
                    throw new ProviderException(ExceptionMessage);
                }

                throw;
            }

            return item;
        }

        private SessionStateStoreData Deserialize(HttpContext context,
            string serializedItems, int timeout)
        {
            using (var ms =
                new MemoryStream(Convert.FromBase64String(serializedItems)))
            {
                var sessionItems =
                    new SessionStateItemCollection();

                if (ms.Length > 0)
                {
                    using (var reader = new BinaryReader(ms))
                    {
                        sessionItems = SessionStateItemCollection.Deserialize(reader);
                    }
                }

                return new SessionStateStoreData(sessionItems,
                    SessionStateUtility.GetSessionStaticObjects(context),
                    timeout);
            }
        }

        public override void CreateUninitializedItem(System.Web.HttpContext context, string id, int timeout)
        {
            var cTime = DateTime.UtcNow;
            var doc = new BsonDocument()
                .Add("_id", id)
                .Add("App", ApplicationName)
                .Add("Created", cTime)
                .Add("Expires", cTime.AddMinutes(timeout))
                .Add("LockDate", cTime)
                .Add("LockId", 0)
                .Add("Timeout", timeout)
                .Add("Locked", false)
                .Add("Items", "")
                .Add("Flags", 1);

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Insert(doc, writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "CreateUninitializedItem");
                    throw new ProviderException(ExceptionMessage);
                }

                throw;
            }
        }

        /// <summary>
        ///     This is a helper function that writes exception detail to the
        ///     event log. Exceptions are written to the event log as a security
        ///     measure to ensure private database details are not returned to
        ///     browser. If a method does not return a status or Boolean
        ///     indicating the action succeeded or failed, the caller also
        ///     throws a generic exception.
        /// </summary>
        private void WriteToEventLog(Exception e, string action)
        {
            using (var log = new EventLog())
            {
                log.Source = EventSource;
                log.Log = EventLog;

                var message =
                    String.Format(
                        "An exception occurred communicating with the data source.\n\nAction: {0}\n\nException: {1}",
                        action, e);

                log.WriteEntry(message);
            }
        }

        public override void Dispose()
        {
        }

        public override void EndRequest(System.Web.HttpContext context)
        {
        }

        public override void InitializeRequest(System.Web.HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(System.Web.HttpContext context, string id, object lockId)
        {
            var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName),
                Query.EQ("LockId", (Int32) lockId));
            var update = Update.Set("Locked", false);
            update.Set("Expires", DateTime.UtcNow.AddMinutes(config.Timeout.TotalMinutes));

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Update(query, update, writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ReleaseItemExclusive");
                    throw new ProviderException(ExceptionMessage);
                }
                throw;
            }
        }

        public override void RemoveItem(System.Web.HttpContext context, string id, object lockId,
            SessionStateStoreData item)
        {
            var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName),
                Query.EQ("LockId", (Int32) lockId));

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Remove(query, writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "RemoveItem");
                    throw new ProviderException(ExceptionMessage);
                }

                throw;
            }
        }

        public override void ResetItemTimeout(System.Web.HttpContext context, string id)
        {
            var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
            var update = Update.Set("Expires", DateTime.UtcNow.AddMinutes(config.Timeout.TotalMinutes));

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Update(query, update, writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ResetItemTimeout");
                    throw new ProviderException(ExceptionMessage);
                }
                throw;
            }
        }
    }
}