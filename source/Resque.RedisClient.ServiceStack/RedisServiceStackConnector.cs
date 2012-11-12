using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Redis;

namespace Resque.RedisClient.ServiceStack
{
    public class RedisServiceStackConnector : IRedis, IDisposable
    {
        private IRedisClient Client { get; set; }
        public string RedisNamespace { get; set; }
        public int RedisDb { get; set; }

        public RedisServiceStackConnector(IRedisClient client, int redisDb = 0, string redisNamespace = "resque")
        {
            Client = client;
            RedisDb = redisDb;
            RedisNamespace = redisNamespace;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;

            if (Client != null)
            {
                try
                {
                    Client.Dispose();
                }
                catch
                {
                }
                Client = null;
            }
        }


        public string KeyInNamespace(string key)
        {
            return string.Join(":", RedisNamespace, key);
        }

        public string[] KeyInNamespace(params string[] keys)
        {
            return keys.Select(x => string.Join(":", RedisNamespace, x)).ToArray();
        }

        public bool SAdd(string key, string redisId)
        {
            Client.AddItemToSet(KeyInNamespace(key), redisId);
            return true;
        }

        public string LPop(string key)
        {
            return Client.PopItemFromList(KeyInNamespace(key));
        }

        public Tuple<string, string> BLPop(string[] keys, int timeoutSeconds)
        {
            var rv = Client.BlockingDequeueItemFromLists(KeyInNamespace(keys),
                                                    timeoutSeconds == 0
                                                        ? (TimeSpan?) null
                                                        : TimeSpan.FromSeconds(timeoutSeconds));
            if(rv == null)
                return null;
            return new Tuple<string, string>(rv.Id, rv.Item);
        }

        public Dictionary<string, string> HGetAll(string key)
        {
            return Client.GetAllEntriesFromHash(KeyInNamespace(key));
        }

        public string HGet(string key, string field)
        {
            return Client.GetValueFromHash(KeyInNamespace(key), field);
        }

        bool IRedis.HSet(string key, string field, string value)
        {
            return Client.SetEntryInHash(KeyInNamespace(key), field, value);
        }

        public bool ZAdd(string key, string value, long score)
        {
            return Client.AddItemToSortedSet(KeyInNamespace(key), value, score);
        }

        public long ZCard(string key)
        {
            return Client.GetSortedSetCount(KeyInNamespace(key));
        }

        public long ZCard(string key, long min, long max)
        {
//            return Client.GetSortedSetCount(KeyInNamespace(key), min.ToString(), max.ToString());
            throw new NotImplementedException("Awaiting release of updated ServiceStack.Redis");
        }

        public Tuple<string, double>[] ZRange(string key, long start, long stop, bool @ascending = false)
        {
//            return Client.GetRangeFromSortedSet(KeyInNamespace(key), (int) start, (int) stop);
            throw new NotImplementedException();
        }

        public double ZScore(string key, string member)
        {
            throw new NotImplementedException();
        }

        public void HSet(string key, string field, string value)
        {
            Client.SetEntryInHash(KeyInNamespace(key), field, value);
        }

        public long Incr(string key)
        {
            return Client.IncrementValue(KeyInNamespace(key));
        }

        public IEnumerable<string> SMembers(string key)
        {
            return Client.GetAllItemsFromSet(KeyInNamespace(key));
        }

        public bool Exists(string key)
        {
            return Client.ContainsKey(KeyInNamespace(key));
        }

        public string Get(string key)
        {
            return Client[KeyInNamespace(key)];
        }

        public void Set(string key, string value)
        {
            Client[KeyInNamespace(key)] = value;
        }

        public long RemoveKeys(params string[] keys)
        {
            Client.RemoveAll(KeyInNamespace(keys));
            return 0;
        }

        public long SRemove(string key, params string[] values)
        {
            foreach (var value in values)
            {
                Client.RemoveItemFromSet(KeyInNamespace(key), value);
            }
            return 0;
        }

        public long RPush(string key, string value)
        {
            Client.AddItemToList(KeyInNamespace(key), value);
            return 0;
        }
    }
}
