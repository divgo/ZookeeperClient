using System;
using System.Collections.Generic;
using ZooKeeperNet;

namespace ZookeeperClient
{
	public class CollectionChangeEventArgs : EventArgs
	{
		public string path { get; set; }
		public CollectionChangeEventArgs(string newPath)
		{
			this.path = newPath;
		}
	}

	public class ZooWatcher : IWatcher
	{

		public delegate void ZooChangeEvent(object sender, CollectionChangeEventArgs e);
		public event ZooChangeEvent WatcherEvent;

		protected virtual void OnChange(CollectionChangeEventArgs e)
		{
		    //Logger.Debug("ZooKeeperClient", "OnChange", "zooWatcher.onChange Called");
            //Logger.Debug("ZooKeeperClient", "OnChange", e.path);

			if (WatcherEvent != null)
			{
                //Logger.Debug("ZooKeeperClient", "OnChange", "zooWatcher.onChange WatcherEvent is not null");
				WatcherEvent(this, e);
			}
			else
			{
                //Logger.Debug("ZooKeeperClient", "OnChange", "zooWatcher.onChange WatcherEvent IS null");
			}
		}

		public void Process(WatchedEvent wEvent)
		{

		    var path = wEvent.Path;
            //Logger.Debug("ZooKeeperClient", "Process", "zooWatcher.Process Called");
            //Logger.Debug("ZooKeeperClient", "Process", "path: " + path);

			if (wEvent.GetType().Equals(EventType.None))
			{
				// We are are being told that the state of the connection has changed
				switch (wEvent.State)
				{
					case KeeperState.SyncConnected:
						// In this particular example we don't need to do anything
						// here - watches are automatically re-registered with 
						// server and any watches triggered while the client was 
						// disconnected will be delivered (in order of course)
						break;
					case KeeperState.Expired:
						// It's all over
						// dead = true;
						// listener.closing(KeeperException.Code.SessionExpired);
						break;
				}
			}
			else
			{
				if (!string.IsNullOrEmpty(path))
				{
					OnChange(new CollectionChangeEventArgs(path));
				}
			}
			var state = wEvent.State;
			var eType = wEvent.Type;
			var ewPath = wEvent.Wrapper.Path;
			var ewState = wEvent.Wrapper.State;
			var ewType = wEvent.Wrapper.Type;
		}
	}
	public class Zoo
	{
		private ZooKeeper zk;
		private string zk_hosts;
		private ClientConnection connSignal;

		public Zoo(string conn)
		{
			this.zk_hosts = conn;
			this.Connect(this.zk_hosts);
		}
		public Zoo(string conn, ZooWatcher watcher)
		{
			this.zk_hosts = conn;
			this.Connect(this.zk_hosts, watcher);
		}

		public void Connect(string host) // ZooKeeper 
		{
			zk = new ZooKeeper(host, System.TimeSpan.FromSeconds(3000), null); //  new ZooWatcher());
		}
		public void Connect(string host, ZooWatcher watcher) // ZooKeeper 
		{
			zk = new ZooKeeper(host, System.TimeSpan.FromSeconds(3000), watcher);
		}
		public void Close()
		{
			if (Equals(zk.State, ZooKeeper.States.CONNECTED))
			{
				zk.Dispose();
			}

			zk = null;
		}
		public void CreateNode(string path, byte[] data)
		{
			if (!string.IsNullOrEmpty(path))
			{
				zk.Create(path, data, ZooKeeperNet.Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
			};
		}
		public void UpdateNode(string path, byte[] data)
		{
			if (!string.IsNullOrEmpty(path))
			{
				zk.SetData(path, data, zk.Exists(path, true).Version);
			}
		}
		public void DeleteNode(string path)
		{
			if (!string.IsNullOrEmpty(path))
			{
				zk.Delete(path, zk.Exists(path, true).Version);
			}
		}

		// GETTERS

		public byte[] GetNodeDataRaw(string path, bool watch)
		{
			byte[] result = null;
			if (!string.IsNullOrEmpty(path))
			{
				try
				{
					result = zk.GetData(path, watch, zk.Exists(path, false));
				}
				catch (KeeperException kex)
				{
                    // always log your exceptions!
				    //Logger.Error("ZooKeeperClient", "Exception occurred at getNodeDataRaw", kex);
                    this.Close();
					return null;
				}
			}
			return result;
		}
		public string GetNodeDataString(string path, bool watch)
		{
			byte[] result = null;

			if (!String.IsNullOrEmpty(path))
			{
				try
				{
					result = zk.GetData(path, watch, zk.Exists(path, false));
				}
				catch (KeeperException kex)
				{
                    //Logger.Error("ZooKeeperClient", "Exception occurred at getNodeDataRaw", kex);
                    this.Close();
				}
			}

			if (result != null && result.Length > 0)
			{
				return System.Text.Encoding.UTF8.GetString(result);
			}
			else
			{
                // is this an error?  What if result was null?
			    //Logger.Debug("ZooKeeperClient", "GetNodeDataString", result == null ? "result was null" : "result was:" + result.Length);
                return null;
			}
		}
		public IEnumerable<string> GetNodeChildren(string path, bool watch)
		{
			return zk.GetChildren(path, watch, zk.Exists(path, false));
		}
	}
}
