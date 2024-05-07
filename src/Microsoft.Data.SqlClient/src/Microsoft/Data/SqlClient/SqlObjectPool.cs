﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Microsoft.Data.SqlClient
{
    // This is a collection of general object pools that can be reused as needed.
    internal static class SqlObjectPools
    {
        private static SqlObjectPool<ArrayBufferWriter<byte>> _bufferWriter;

        internal static SqlObjectPool<ArrayBufferWriter<byte>> BufferWriter
        {
            get
            {
                if (_bufferWriter is null)
                {
                    Interlocked.CompareExchange(ref _bufferWriter, new(20, () => new(), a => a.Clear()), null);
                }

                return _bufferWriter;
            }
        }
    }

#if NETSTANDARD || NETFRAMEWORK
    internal static class BufferWriterExtensions
    {
        internal static long GetBytes(this Encoding encoding, string str, IBufferWriter<byte> bufferWriter)
        {
            var count = encoding.GetByteCount(str);
            var array = ArrayPool<byte>.Shared.Rent(count);

            try
            {
                encoding.GetBytes(str, 0, str.Length, array, 0);
                bufferWriter.Write(array);
                return count;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(array);
            }
        }
    }
#endif

    // this is a very simple threadsafe pool derived from the aspnet/extensions default pool implementation
    // https://github.com/dotnet/extensions/blob/release/3.1/src/ObjectPool/src/DefaultObjectPool.cs
    internal sealed class SqlObjectPool<T> where T : class
    {
        private readonly ObjectWrapper[] _items;
        private readonly Action<T> _onReturned;
        private readonly Func<T> _onCreate;

        private T _firstItem;

        public SqlObjectPool(int maximumRetained, Func<T> onCreate = null, Action<T> onReturned = null)
        {
            // -1 due to _firstItem
            _items = new ObjectWrapper[maximumRetained - 1];
            _onReturned = onReturned;
            _onCreate = onCreate;
        }

        public T Rent()
        {
            if (TryGet(out var item))
            {
                return item;
            }

            return _onCreate?.Invoke() ?? throw new InvalidOperationException("Can only rent from a pool if an onCreate delegate is available");
        }

        public bool TryGet(out T item)
        {
            item = null;
            T taken = _firstItem;
            if (taken != null && Interlocked.CompareExchange(ref _firstItem, null, taken) == taken)
            {
                // took first item
                item = taken;
                return true;
            }
            else
            {
                var items = _items;
                for (var i = 0; i < items.Length; i++)
                {
                    taken = items[i].Element;
                    if (taken != null && Interlocked.CompareExchange(ref items[i].Element, null, taken) == taken)
                    {
                        item = taken;
                        return true;
                    }
                }
            }
            return false;
        }

        public void Return(T item)
        {
            _onReturned?.Invoke(item);

            if (_firstItem != null || Interlocked.CompareExchange(ref _firstItem, item, null) != null)
            {
                var items = _items;
                for (var i = 0; i < items.Length && Interlocked.CompareExchange(ref items[i].Element, item, null) != null; ++i)
                {
                }
            }
        }

        // PERF: the struct wrapper avoids array-covariance-checks from the runtime when assigning to elements of the array.
        [DebuggerDisplay("{Element}")]
        private struct ObjectWrapper
        {
            public T Element;
        }
    }
}
