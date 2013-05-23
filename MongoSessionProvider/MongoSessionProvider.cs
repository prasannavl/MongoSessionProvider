using System;
using System.Collections.Generic;
using System.Text;
using System.Web.SessionState;
using System.Configuration;
using System.Configuration.Provider;
using System.Web.Configuration;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Diagnostics;
using System.IO;
using MongoDB.Driver.Builders;
using System.Web;

namespace PVL
{
    /// <summary>
    /// Custom ASP.NET Session State Provider using MongoDB as the state store.

    /// Session state default store: 
    /// "Sessions" collection within a "SessionState" database. 
    /// 
    /// Example session document:
    /// {
    ///    "_id" : "i2guetwsm0mgaibb1gqmodfq",
    ///    "App" : "/",
    ///    "Created" : ISODate("2013-02-21T22:27:32.091Z"),
    ///    "Expires" : ISODate("2013-02-22T22:30:59.267Z"),
    ///    "LockDate" : ISODate("2013-02-21T22:29:54.481Z"),
    ///    "LockId" : 1,
    ///    "Timeout" : 20,
    ///    "Locked" : true,
    ///    "Items" : "AQAAAP////8EVGVzdAgAAAABBkFkcmlhbg==",
    ///    "Flags" : 0
    /// }
    /// 
    /// Scheduled session cleanup:
    /// db.Sessions.remove({"Expires" : {$lt : new Date() }})
    /// 
    /// Example web.config settings:
    ///  
    /// ..
    /// <connectionStrings>
    /// <add name="SessionState" connectionString="mongodb://localhost"/>
    /// </connectionStrings>
    /// <system.web>
    ///     <sessionState mode="Custom" timeout="1440" cookieless="false" customProvider="MongoSessionStateProvider">
    ///         <providers>
    ///             <add name="MongoSessionStateProvider" type="PVL.MongoSessionProvider" connectionStringName="SessionState" writeExceptionsToEventLog="false"/>
    ///         </providers>
    ///     </sessionState>
    /// </system.web>
    /// ..
    /// </summary>
    /// 


    public sealed class MongoSessionProvider : SessionStateStoreProviderBase
    {
        private SessionStateSection _config = null;
        private ConnectionStringSettings _connectionStringSettings;
        private String _databaseName;
        private String _collectionName;
        private string _applicationName;
        private string _connectionString;
        private WriteConcern _writeMode = null;
        private bool _writeExceptionsToEventLog;
        private const string _exceptionMessage = "An error occurred. Please contact support if the problem persists.";
        private const string _eventSource = "MongoSessionStateStore";
        private const string _eventLog = "Application";


        public string ApplicationName
        {
            get { return _applicationName; }
        }


        public bool WriteExceptionsToEventLog
        {
            get { return _writeExceptionsToEventLog; }
            set { _writeExceptionsToEventLog = value; }
        }


        private MongoCollection<BsonDocument> GetSessionCollection()
        {
            MongoClient client = new MongoClient(_connectionString);
            return client.GetServer().GetDatabase(_databaseName).GetCollection(_collectionName);
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
            _applicationName = System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath;

            // Get <sessionState> configuration element.
            Configuration cfg = WebConfigurationManager.OpenWebConfiguration(ApplicationName);
            _config = (SessionStateSection)cfg.GetSection("system.web/sessionState");

            // Initialize connection string.
            _connectionStringSettings = ConfigurationManager.ConnectionStrings[config["connectionStringName"]];
            _databaseName = config["databaseName"] ?? "SessionState";
            _collectionName = config["collectionName"] ?? "Sessions";

            if (_connectionStringSettings == null || _connectionStringSettings.ConnectionString.Trim() == "")
            {
                throw new ProviderException("Connection string cannot be blank.");
            }

            _connectionString = _connectionStringSettings.ConnectionString;

            // Initialize WriteExceptionsToEventLog
            _writeExceptionsToEventLog = false;

            if (config["writeExceptionsToEventLog"] != null)
            {
                if (config["writeExceptionsToEventLog"].ToUpper() == "TRUE")
                    _writeExceptionsToEventLog = true;
            }

            _writeMode = WriteConcern.Unacknowledged;

            if (config["writeConcern"] != null)
            {
                int result;
                if (int.TryParse(config["writeConcern"], out result))
                    if ((result > -2))
                        _writeMode.W = result;
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
        /// Serialize is called by the SetAndReleaseItemExclusive method to 
        /// convert the SessionStateItemCollection into a Base64 string to    
        /// be stored in MongoDB.
        /// </summary>
        private string Serialize(SessionStateItemCollection items)
        {
            using (MemoryStream ms = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(ms))
            {
                if (items != null)
                    items.Serialize(writer);

                writer.Close();

                return Convert.ToBase64String(ms.ToArray());
            }
        }

        /// <summary>
        /// SessionStateProviderBase.SetAndReleaseItemExclusive
        /// </summary>
        public override void SetAndReleaseItemExclusive(HttpContext context,
          string id,
          SessionStateStoreData item,
          object lockId,
          bool newItem)
        {
            // Serialize the SessionStateItemCollection as a string.
            string sessItems = Serialize((SessionStateItemCollection)item.Items);
            DateTime cTime = DateTime.UtcNow;
            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                if (newItem)
                {
                    BsonDocument doc = new BsonDocument()
                                .Add("_id", id)
                                .Add("App", ApplicationName)
                                .Add("Created", cTime)
                                .Add("Expires", cTime.AddMinutes((Double)item.Timeout))
                                .Add("LockDate", cTime)
                                .Add("LockId", 0)
                                .Add("Timeout", item.Timeout)
                                .Add("Locked", false)
                                .Add("Items", "")
                                .Add("Flags", 1);

                    sessionCollection.Save(doc, _writeMode);
                }
                else
                {
                    var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName), Query.EQ("LockId", (Int32)lockId));
                    var update = Update.Set("Expires", cTime.AddMinutes((Double)item.Timeout));
                    update.Set("Items", sessItems);
                    update.Set("Locked", false);
                    sessionCollection.Update(query, update, _writeMode);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "SetAndReleaseItemExclusive");
                    throw new ProviderException(_exceptionMessage);
                }
                throw;
            }
        }

        /// <summary>
        /// SessionStateProviderBase.GetItem
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
        /// SessionStateProviderBase.GetItemExclusive
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
        /// GetSessionStoreItem is called by both the GetItem and 
        /// GetItemExclusive methods. GetSessionStoreItem retrieves the 
        /// session data from the data source. If the lockRecord parameter
        /// is true (in the case of GetItemExclusive), then GetSessionStoreItem
        /// locks the record and sets a new LockId and LockDate.
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
            DateTime expires;
            // String to hold serialized SessionStateItemCollection.
            string serializedItems = "";
            // True if a record is found in the database.
            bool foundRecord = false;
            // True if the returned session item is expired and needs to be deleted.
            bool deleteData = false;
            // Timeout value from the data store.
            int timeout = 0;
            IMongoQuery query = null;
            DateTime cTime = DateTime.UtcNow;
            try
            {
                MongoCollection sessionCollection = GetSessionCollection();

                // lockRecord is true when called from GetItemExclusive and
                // false when called from GetItem.
                // Obtain a lock if possible. Ignore the record if it is expired.
                if (lockRecord)
                {
                    query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName), Query.EQ("Locked", false), Query.GT("Expires", cTime));
                    var update = Update.Set("Locked", true);
                    update.Set("LockDate", cTime);
                    var result = sessionCollection.Update(query, update);

                    if (result.DocumentsAffected == 0)
                    {
                        // No record was updated because the record was locked or not found.
                        locked = true;
                    }
                    else
                    {
                        // The record was updated.
                        locked = false;
                    }
                }

                // Retrieve the current session item information.
                query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
                var results = sessionCollection.FindOneAs<BsonDocument>(query);

                if (results != null)
                {
                    expires = results["Expires"].AsDateTime;

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
                    actionFlags = (SessionStateActions)results["Flags"].AsInt32;
                    timeout = results["Timeout"].AsInt32;
                }

                // If the returned session item is expired, 
                // delete the record from the data source.
                if (deleteData)
                {
                    query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
                    sessionCollection.Remove(query, _writeMode);
                }

                // The record was not found. Ensure that locked is false.
                if (!foundRecord)
                    locked = false;

                // If the record was found and you obtained a lock, then set 
                // the lockId, clear the actionFlags,
                // and create the SessionStateStoreItem to return.
                if (foundRecord && !locked)
                {
                    lockId = (int)lockId + 1;

                    query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
                    var update = Update.Set("LockId", (int)lockId);
                    update.Set("Flags", 0);
                    sessionCollection.Update(query, update, _writeMode);

                    // If the actionFlags parameter is not InitializeItem, 
                    // deserialize the stored SessionStateItemCollection.
                    if (actionFlags == SessionStateActions.InitializeItem)
                        item = CreateNewStoreData(context, (int)_config.Timeout.TotalMinutes);
                    else
                        item = Deserialize(context, serializedItems, timeout);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "GetSessionStoreItem");
                    throw new ProviderException(_exceptionMessage);
                }

                throw;
            }

            return item;
        }

        private SessionStateStoreData Deserialize(HttpContext context,
         string serializedItems, int timeout)
        {
            using (MemoryStream ms =
              new MemoryStream(Convert.FromBase64String(serializedItems)))
            {

                SessionStateItemCollection sessionItems =
                  new SessionStateItemCollection();

                if (ms.Length > 0)
                {
                    using (BinaryReader reader = new BinaryReader(ms))
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
            DateTime cTime = DateTime.UtcNow;
            BsonDocument doc = new BsonDocument()
                                .Add("_id", id)
                                .Add("App", ApplicationName)
                                .Add("Created", cTime)
                                .Add("Expires", cTime.AddMinutes((Double)timeout))
                                .Add("LockDate", cTime)
                                .Add("LockId", 0)
                                .Add("Timeout", timeout)
                                .Add("Locked", false)
                                .Add("Items", "")
                                .Add("Flags", 1);

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Insert(doc, _writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "CreateUninitializedItem");
                    throw new ProviderException(_exceptionMessage);
                }

                throw;
            }
        }

        /// <summary>
        /// This is a helper function that writes exception detail to the 
        /// event log. Exceptions are written to the event log as a security
        /// measure to ensure private database details are not returned to 
        /// browser. If a method does not return a status or Boolean
        /// indicating the action succeeded or failed, the caller also 
        /// throws a generic exception.
        /// </summary>
        private void WriteToEventLog(Exception e, string action)
        {
            using (EventLog log = new EventLog())
            {
                log.Source = _eventSource;
                log.Log = _eventLog;

                string message =
                  String.Format("An exception occurred communicating with the data source.\n\nAction: {0}\n\nException: {1}",
                  action, e.ToString());

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

            var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName), Query.EQ("LockId", (Int32)lockId));
            var update = Update.Set("Locked", false);
            update.Set("Expires", DateTime.UtcNow.AddMinutes(_config.Timeout.TotalMinutes));

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Update(query, update, _writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ReleaseItemExclusive");
                    throw new ProviderException(_exceptionMessage);
                }
                throw;
            }
        }

        public override void RemoveItem(System.Web.HttpContext context, string id, object lockId, SessionStateStoreData item)
        {


            var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName), Query.EQ("LockId", (Int32)lockId));

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Remove(query, _writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "RemoveItem");
                    throw new ProviderException(_exceptionMessage);
                }

                throw;
            }
        }

        public override void ResetItemTimeout(System.Web.HttpContext context, string id)
        {

            var query = Query.And(Query.EQ("_id", id), Query.EQ("App", ApplicationName));
            var update = Update.Set("Expires", DateTime.UtcNow.AddMinutes(_config.Timeout.TotalMinutes));

            try
            {
                MongoCollection sessionCollection = GetSessionCollection();
                sessionCollection.Update(query, update, _writeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ResetItemTimeout");
                    throw new ProviderException(_exceptionMessage);
                }
                throw;
            }
        }
    }
}
