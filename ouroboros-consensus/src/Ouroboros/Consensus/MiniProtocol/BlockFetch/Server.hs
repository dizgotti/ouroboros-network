{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE EmptyDataDeriving         #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE PatternSynonyms           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE UndecidableInstances      #-}
module Ouroboros.Consensus.MiniProtocol.BlockFetch.Server
  ( blockFetchServer
    -- * Trace events
  , TraceBlockFetchServerEvent
    -- * Exceptions
  , BlockFetchServerException
  ) where

import           Control.Tracer (Tracer, traceWith)
import           Data.Proxy (Proxy (..))
import           Data.Typeable ((:~:) (..), Typeable, eqT)

import           Ouroboros.Network.Block (pattern BlockPoint, HeaderHash,
                     Serialised (..), StandardHash)
import           Ouroboros.Network.Protocol.BlockFetch.Server
                     (BlockFetchBlockSender (..), BlockFetchSendBlocks (..),
                     BlockFetchServer (..))
import           Ouroboros.Network.Protocol.BlockFetch.Type (ChainRange (..))

import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Util.IOLike
import           Ouroboros.Consensus.Util.ResourceRegistry (ResourceRegistry)

import           Ouroboros.Consensus.Storage.ChainDB (ChainDB,
                     IteratorResult (..), SerialisedWithPoint (..),
                     getSerialisedBlockWithPoint)
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB

data BlockFetchServerException =
      -- | A block that was supposed to be included in a batch was garbage
      -- collected since we started the batch and can no longer be sent.
      --
      -- This will very rarely happen, only in the following scenario: when
      -- the batch started, the requested blocks were on the current chain,
      -- but then the current chain changed such that the requested blocks are
      -- now on a fork. If while requesting the blocks from the batch, there
      -- were a pause of /hours/ such that the fork gets older than @k@, then
      -- the next request after this long pause could result in this
      -- exception, as the block to stream from the old fork could have been
      -- garbage collected. However, the network protocol will have timed out
      -- long before this happens.
      forall blk. (Typeable blk, StandardHash blk) =>
        BlockGCed (Proxy blk) (HeaderHash blk)

      -- | Thrown when requesting the genesis block from the database
      --
      -- Although the genesis block has a hash and a point associated with it,
      -- it does not actually exist other than as a concept; we cannot read and
      -- return it.
    | NoGenesisBlock

deriving instance Show BlockFetchServerException

instance Eq BlockFetchServerException where
  BlockGCed (_ :: Proxy blk1) h1 == BlockGCed (_ :: Proxy blk2) h2 =
      case eqT @blk1 @blk2 of
        Nothing   -> False
        Just Refl -> h1 == h2
  NoGenesisBlock                 == NoGenesisBlock                 = True
  _                              == _                              = False

instance Exception BlockFetchServerException

-- | Block fetch server based on
-- 'Ouroboros.Network.BlockFetch.Examples.mockBlockFetchServer1', but using
-- the 'ChainDB'.
blockFetchServer
    :: forall m blk.
       ( IOLike m
       , StandardHash blk
       , Typeable     blk
       )
    => Tracer m (TraceBlockFetchServerEvent blk)
    -> ChainDB m blk
    -> ResourceRegistry m
    -> BlockFetchServer (Serialised blk) m ()
blockFetchServer _tracer chainDB registry = senderSide
  where
    senderSide :: BlockFetchServer (Serialised blk) m ()
    senderSide = BlockFetchServer receiveReq' ()

    receiveReq' :: ChainRange (Serialised blk)
                -> m (BlockFetchBlockSender (Serialised blk) m ())
    receiveReq' (ChainRange start end) =
      case (start, end) of
        (BlockPoint s h, BlockPoint s' h') ->
          receiveReq (RealPoint s h) (RealPoint s' h')
        _otherwise ->
          throwM NoGenesisBlock

    receiveReq :: RealPoint blk
               -> RealPoint blk
               -> m (BlockFetchBlockSender (Serialised blk) m ())
    receiveReq start end = do
      traceWith _tracer $ OpeningIterator start end
      errIt <- ChainDB.stream
        chainDB
        registry
        getSerialisedBlockWithPoint
        (ChainDB.StreamFromInclusive start)
        (ChainDB.StreamToInclusive   end)
      traceWith _tracer $ errIt `seq` OpenedIterator start end
      return $ case errIt of
        -- The range is not in the ChainDB or it forks off more than @k@
        -- blocks back.
        Left  _  -> SendMsgNoBlocks $ return senderSide
        -- When we got an iterator, it will stream at least one block since
        -- its bounds are inclusive, so we don't have to check whether the
        -- iterator is empty.
        Right it -> SendMsgStartBatch $ sendBlocks it

    sendBlocks :: ChainDB.Iterator m blk (SerialisedWithPoint blk blk)
               -> m (BlockFetchSendBlocks (Serialised blk) m ())
    sendBlocks it = do
      traceWith _tracer $ it `seq` AdvancingIterator
      next <- ChainDB.iteratorNext it
      traceWith _tracer $ next `seq` AdvancedIterator
      case next of
        IteratorResult blk     ->
          return $ SendMsgBlock (serialised blk) (sendBlocks it)
        IteratorExhausted      -> do
          ChainDB.iteratorClose it
          return $ SendMsgBatchDone $ return senderSide
        IteratorBlockGCed hash -> do
          ChainDB.iteratorClose it
          throwM $ BlockGCed (Proxy @blk) hash


{-------------------------------------------------------------------------------
  Trace events
-------------------------------------------------------------------------------}

-- | Events traced by the Block Fetch Server.
data TraceBlockFetchServerEvent blk =
   -- TODO no events yet. Tracing the messages send/received over the network
   -- might be all we need?
    OpeningIterator (RealPoint blk) (RealPoint blk)
  |
    OpenedIterator (RealPoint blk) (RealPoint blk)
  |
    AdvancingIterator
  |
    AdvancedIterator
  deriving (Eq, Show)