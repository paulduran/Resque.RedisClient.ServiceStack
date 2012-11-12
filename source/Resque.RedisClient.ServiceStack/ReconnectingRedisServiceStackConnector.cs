using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Redis;

namespace Resque.RedisClient.ServiceStack
{
    public class ReconnectingRedisServiceStackConnector : IRedis, IDisposable
    {
        private IRedisClientsManager ClientManager { get; set; }
        private RedisServiceStackConnector connector;
        public string RedisNamespace { get; set; }
        public int RedisDb { get; set; }

        public ReconnectingRedisServiceStackConnector(IRedisClientsManager clientManager, int redisDb = 0, string redisNamespace = "resque")
        {
            ClientManager = clientManager;
            RedisDb = redisDb;
            RedisNamespace = redisNamespace;
        }

        public RedisServiceStackConnector Client
        {
            get {
                if (connector == null)
                {
                    connector =
                            new RedisServiceStackConnector(ClientManager.GetClient(), RedisDb, RedisNamespace);
                }
                return connector;
            }
        }

        private void DisposeConnection()
        {
            if (connector != null)
            {
                try
                {
                    connector.Dispose();
                }
                catch
                {
                }
            }
            connector = null; // ensure we create a new client next time
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;

            DisposeConnection();
        }

        public T Do<T>(Func<IRedis, T> func)
        {
            try
            {
                return func(Client);
            }
            catch (RedisException)
            {
                DisposeConnection();
                throw;
            }
        }

        public void Do(Action<IRedis> action)
        {
            try
            {
                action(Client);
            }
            catch (RedisException)
            {
                DisposeConnection();
                throw;
            }
        }

        public string LPop(string key)
        {
            return Do(client => client.LPop(key));
        }

        public void Set(string key, string value)
        {
            Do(client => client.Set(key, value));
        }

        public long RemoveKeys(params string[] keys)
        {
            return Do(client => client.RemoveKeys(keys));
        }

        public bool Exists(string key)
        {
            return Do(client => client.Exists(key));
        }

        public string Get(string key)
        {
            return Do(client => client.Get(key));
        }

        public bool SAdd(string key, string redisId)
        {
            return Do(client => client.SAdd(key, redisId));
        }

        public long SRemove(string key, params string[] values)
        {
            return Do(client => client.SRemove(key, values));
        }

        public IEnumerable<string> SMembers(string key)
        {
            return Do(client => client.SMembers(key));
        }

        public long RPush(string key, string value)
        {
            return Do(client => client.RPush(key, value));
        }

        public Tuple<string, string> BLPop(string[] keys, int timeoutSeconds)
        {
            return Do(client => client.BLPop(keys, timeoutSeconds));
        }

        public Dictionary<string, string> HGetAll(string key)
        {
            return Do(client => client.HGetAll(key));
        }

        public string HGet(string key, string field)
        {
            return Do(client => client.HGet(key,field));
        }

        public bool HSet(string key, string field, string value)
        {
            return Do(client => client.HSet(key, field, value));
        }

        public bool ZAdd(string key, string value, long score)
        {
            return Do(client => client.ZAdd(key, value, score));
        }

        public long ZCard(string key)
        {
            return Do(client => client.ZCard(key));
        }

        public long ZCard(string key, long min, long max)
        {
            return Do(client => client.ZCard(key, min, max));
        }

        public Tuple<string, double>[] ZRange(string key, long start, long stop, bool @ascending = false)
        {
            return Do(client => client.ZRange(key, start, stop, @ascending));
        }

        public double ZScore(string key, string member)
        {
            return Do(client => client.ZScore(key, member));
        }

        public long Incr(string key)
        {
            return Do(client => client.Incr(key));
        }
    }
}
