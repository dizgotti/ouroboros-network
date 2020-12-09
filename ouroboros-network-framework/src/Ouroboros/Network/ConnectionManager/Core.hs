{-# LANGUAGE BangPatterns         #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveFoldable       #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE KindSignatures       #-}
{-# LANGUAGE NamedFieldPuns       #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
-- Undecidable instances are need for 'Show' instance of 'ConnectionState'.
{-# LANGUAGE UndecidableInstances #-}

-- | The implementation of connection manager.
--
module Ouroboros.Network.ConnectionManager.Core
  ( ConnectionManagerArguments (..)
  , withConnectionManager
  , defaultTimeWaitTimeout
  , defaultProtocolIdleTimeout
  ) where

import           Control.Exception (assert)
import           Control.Monad.Class.MonadFork
import           Control.Monad.Class.MonadAsync
import           Control.Monad.Class.MonadThrow hiding (handle)
import           Control.Monad.Class.MonadTimer
import           Control.Monad.Class.MonadSTM.Strict
import           Control.Tracer (Tracer, traceWith, contramap)
import           Data.Foldable (traverse_)
import           Data.Functor (($>))
import           Data.Maybe (maybeToList)
import           Data.Proxy (Proxy (..))
import           Data.Typeable (Typeable)
import           GHC.Stack (CallStack, HasCallStack, callStack)

import           Data.Map (Map)
import qualified Data.Map as Map

import           Network.Mux.Types (MuxMode)
import           Network.Mux.Trace (MuxTrace, WithMuxBearer (..))

import           Ouroboros.Network.ConnectionId
import           Ouroboros.Network.ConnectionManager.Types
import           Ouroboros.Network.Snocket
import           Ouroboros.Network.Server.RateLimiting (AcceptedConnectionsLimit (..))


-- | Arguments for a 'ConnectionManager' which are independent of 'MuxMode'.
--
data ConnectionManagerArguments handlerTrace socket peerAddr version m =
    ConnectionManagerArguments {
        -- | Connection manager tracer.
        --
        cmTracer              :: Tracer m (ConnectionManagerTrace peerAddr handlerTrace),

        -- | Mux trace.
        --
        cmMuxTracer           :: Tracer m (WithMuxBearer (ConnectionId peerAddr) MuxTrace),

        -- | @IPv4@ address of the connection manager.  If given, outbound
        -- connections to an @IPv4@ address will bound to it.  To use
        -- bidirectional @TCP@ connections, it must be the same as the server
        -- listening @IPv4@ address.
        --
        cmIPv4Address         :: Maybe peerAddr,

        -- | @IPv6@ address of the connection manager.  If given, outbound
        -- connections to an @IPv6@ address will bound to it.  To use
        -- bidirectional @TCP@ connections, it must be the same as the server
        -- listening @IPv6@ address.
        --
        cmIPv6Address         :: Maybe peerAddr,

        cmAddressType         :: peerAddr -> Maybe AddressType,

        -- | Snocket for the 'socket' type.
        --
        cmSnocket             :: Snocket m socket peerAddr,

        -- | @TCP@ will held connections in @TIME_WAIT@ state for up to two MSL
        -- (maximum segment time).  On Linux this is set to '60' seconds on
        -- other system this might be up to four minutes.
        --
        -- This is configurable, so we can set different value in tests.
        --
        -- When this timeout expires a connection will transition from
        -- 'TerminatingState' to 'TerminatedState'.
        --
        cmTimeWaitTimeout     :: DiffTime,

        -- | @version@ represents the tuple of @versionNumber@ and
        -- @agreedOptions@.
        --
        connectionDataFlow    :: version -> DataFlow,

        -- | Prune policy
        --
        cmPrunePolicy         :: PrunePolicy peerAddr (STM m),
        cmConnectionsLimits   :: AcceptedConnectionsLimit
      }


-- | 'ConnectionManager' state: for each peer we keep a 'ConnectionState' in
-- a mutable variable, which reduce congestion on the 'TMVar' which keeps
-- 'ConnectionManagerState'.
--
-- It is important we can lookup by remote @peerAddr@; this way we can find if
-- the connection manager is already managing a connection towards that
-- @peerAddr@ and reuse the 'ConnectionState'.
--
type ConnectionManagerState peerAddr handle handleError version m
  = Map peerAddr (StrictTVar m (ConnectionState peerAddr handle handleError version m))


-- | State of a connection.
--
data ConnectionState peerAddr handle handleError version m =
    -- | Each outbound connections starts in this state.
    ReservedOutboundState

    -- | Each inbound connection starts in this state, outbound connection
    -- reach this state once `connect` call returns.
  | UnnegotiatedState   !Provenance
                        !(ConnectionId peerAddr)
                        !(Async m ())

    -- | @OutboundState Unidirectional@ state.
  | OutboundUniState    !(ConnectionId peerAddr) !(Async m ()) !handle

    -- | Either @OutboundState Duplex@ or @OutobundState^\tau Duplex@.
  | OutboundDupState    !(ConnectionId peerAddr) !(Async m ()) !handle !TimeoutExpired
  | InboundIdleState    !(ConnectionId peerAddr) !(Async m ()) !handle !DataFlow
  | InboundState        !(ConnectionId peerAddr) !(Async m ()) !handle !DataFlow
  | DuplexState         !(ConnectionId peerAddr) !(Async m ()) !handle
  | TerminatingState    !(ConnectionId peerAddr) !(Async m ()) !(Maybe handleError)
  | TerminatedState                              !(Maybe handleError)


instance ( Show peerAddr
         , Show handleError
         , Show (ThreadId m)
         , MonadAsync m
         )
      => Show (ConnectionState peerAddr handle handleError version m) where
    show ReservedOutboundState = "ReservedOutboundState"
    show (UnnegotiatedState pr connId connThread) =
      concat ["UnnegotiatedState "
             , show pr
             , " "
             , show connId
             , " "
             , show (asyncThreadId (Proxy :: Proxy m) connThread)
             ]
    show (OutboundUniState connId connThread _handle) =
      concat [ "OutboundState Unidirectional "
             , show connId
             , " "
             , show (asyncThreadId (Proxy :: Proxy m) connThread)
             ]
    show (OutboundDupState connId connThread _handle expired) =
      concat [ "OutboundState "
             , show connId
             , " "
             , show (asyncThreadId (Proxy :: Proxy m) connThread)
             , " "
             , show expired
             ]
    show (InboundIdleState connId connThread _handle df) =
      concat ([ "InboundIdleState "
              , show connId
              , " "
              , show (asyncThreadId (Proxy :: Proxy m) connThread)
              , " "
              , show df
              ])
    show (InboundState  connId connThread _handle df) =
      concat [ "InboundState "
             , show connId
             , " "
             , show (asyncThreadId (Proxy :: Proxy m) connThread)
             , " "
             , show df
             ]
    show (DuplexState   connId connThread _handle) =
      concat [ "DuplexState "
             , show connId
             , " "
             , show (asyncThreadId (Proxy :: Proxy m) connThread)
             ]
    show (TerminatingState connId connThread handleError) =
      concat ([ "TerminatingState "
              , show connId
              , " "
              , show (asyncThreadId (Proxy :: Proxy m) connThread)
              ]
              ++ maybeToList (((' ' :) . show) <$> handleError))
    show (TerminatedState handleError) =
      concat (["TerminatedState"]
              ++ maybeToList (((' ' :) . show) <$> handleError))


getConnThread :: ConnectionState peerAddr handle handleError version m
              -> Maybe (Async m ())
getConnThread ReservedOutboundState                                     = Nothing
getConnThread (UnnegotiatedState _pr   _connId connThread)              = Just connThread
getConnThread (OutboundUniState        _connId connThread _handle )     = Just connThread
getConnThread (OutboundDupState        _connId connThread _handle _te)  = Just connThread
getConnThread (InboundIdleState        _connId connThread _handle _df)  = Just connThread
getConnThread (InboundState            _connId connThread _handle _df)  = Just connThread
getConnThread (DuplexState             _connId connThread _handle)      = Just connThread
getConnThread (TerminatingState        _connId connThread _handleError) = Just connThread
getConnThread TerminatedState {}                                        = Nothing

-- | Get 'DataFlow' for a connection.  It returns 'Nowhere' if that connection
-- is either not yet created or in terminating state, 'There' for  unnegotiated
-- connections and 'Here' if the data flow is known.
--
getConnType :: ConnectionState peerAddr handle handleError version m
            -> Maybe ConnectionType
getConnType ReservedOutboundState                                    = Nothing
getConnType (UnnegotiatedState pr  _connId _connThread)              = Just (UnnegotiatedConn pr)
getConnType (OutboundUniState      _connId _connThread _handle)      = Just (NegotiatedConn Outbound Unidirectional)
getConnType (OutboundDupState      _connId _connThread _handle _te)  = Just (NegotiatedConn Outbound Duplex)
getConnType (InboundIdleState      _connId _connThread _handle df)   = Just (InboundIdleConn df)
getConnType (InboundState          _connId _connThread _handle df)   = Just (NegotiatedConn Inbound df)
getConnType (DuplexState           _connId _connThread _handle)      = Just DuplexConn
getConnType (TerminatingState      _connId _connThread _handleError) = Nothing
getConnType TerminatedState {}                                       = Nothing


summariseState :: ConnectionState muxMode peerAddr m a b -> InState
summariseState ReservedOutboundState {}       = InReservedOutboundState
summariseState UnnegotiatedState {}           = InUnnegotiatedState
summariseState (OutboundUniState    _ _ _)    = InOutboundUniState
summariseState (OutboundDupState    _ _ _ te) = InOutboundDupState te
summariseState (InboundIdleState _ _ _ df)    = InInboundIdleState df
summariseState (InboundState     _ _ _ df)    = InInboundState df
summariseState DuplexState {}                 = InDuplexState
summariseState TerminatingState {}            = InTerminatingState
summariseState TerminatedState {}             = InTerminatedState


-- | The default value for 'cmTimeWaitTimeout'.
--
defaultTimeWaitTimeout :: DiffTime
defaultTimeWaitTimeout = 60

-- | Inactivity timeout.  It configures how long to wait since the local side
-- demoted remote peer to /cold/, before closing the connection.
--
defaultProtocolIdleTimeout :: DiffTime
defaultProtocolIdleTimeout = 5


-- | A wedge product
-- <https://hackage.haskell.org/package/smash/docs/Data-Wedge.html#t:Wedge>
--
data Wedge a b =
    Nowhere
  | Here a
  | There b


-- | Instruction used internally in @unregisterOutboundConectionImpl@, e.g. in
-- the implementation of one of the two  @DemotedToCold^{dataFlow}_{Local}@
-- transitions.
--
data DemoteToColdLocal peerAddr handlerTrace handle handleError version m
    -- | Any @DemotedToCold@ transition which terminates the connection:
    -- @
    --   DemotedToCold^{Duplex}_{Local} : * -> TerminatingState
    -- @
    -- from the spec.
    --
    = DemotedToColdLocal     (ConnectionId peerAddr)
                             (Async m ())
                             InState

    -- | Any @DemoteToCold@ transition which does not terminate the connection, i.e.
    -- @
    --   DemotedToCold^{Duplex}_{Local} : OutboundState^\tau Duplex
    --                                  → InboundIdleState^\tau
    -- @
    -- or the case where the connection is already in 'TerminatingState' or
    -- 'TerminatedState'.
    --
    | DemoteToColdLocalNoop  InState

    -- | Duplex connection was demoted, prune connections.
    --
    | PruneConnections       (ConnectionId peerAddr)
                             (Map peerAddr (Async m ()))

    -- | Demote error.
    | DemoteToColdLocalError (ConnectionManagerTrace peerAddr handlerTrace)
                             InState


-- | Entry point for using the connection manager.  This is a classic @with@ style
-- combinator, which cleans resources on exit of the callback (whether cleanly
-- or through an exception).
--
-- Including a connection (either inbound or outbound) is an idempotent
-- operation on connection manager state.  The connection manager will always
-- return the handle that was first to be included in its state.
--
-- Once an inbound connection is passed to the 'ConnectionManager', the manager
-- is responsible for the resource.
--
withConnectionManager
    :: forall (muxMode :: MuxMode) peerAddr socket handlerTrace handle handleError version m a.
       ( Monad              m
       , MonadLabelledSTM   m
       , MonadAsync         m
       , MonadEvaluate      m
       , MonadMask          m
       , MonadTimer         m
       , MonadThrow    (STM m)

       , Ord      peerAddr
       , Show     peerAddr
       , Typeable peerAddr
       )
    => ConnectionManagerArguments handlerTrace socket peerAddr version m
    -> ConnectionHandler muxMode handlerTrace peerAddr handle handleError version m
    -- ^ Callback which runs in a thread dedicated for a given connection.
    -> (handleError -> HandleErrorType)
    -- ^ classify 'handleError's
    -> (ConnectionManager muxMode socket peerAddr handle handleError m -> m a)
    -- ^ Continuation which receives the 'ConnectionManager'.  It must not leak
    -- outside of scope of this callback.  Once it returns all resources
    -- will be closed.
    -> m a
withConnectionManager ConnectionManagerArguments {
                          cmTracer    = tracer,
                          cmMuxTracer = muxTracer,
                          cmIPv4Address,
                          cmIPv6Address,
                          cmAddressType,
                          cmSnocket,
                          cmTimeWaitTimeout,
                          connectionDataFlow,
                          cmPrunePolicy,
                          cmConnectionsLimits
                        }
                      ConnectionHandler {
                          connectionHandler
                        }
                      classifyHandleError
                      k = do
    (stateVar ::  StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m))
      <- atomically $  do
          v <- newTMVar Map.empty
          labelTMVar v "cm-state"
          return v
    let connectionManager :: ConnectionManager muxMode socket peerAddr
                                               handle handleError m
        connectionManager =
          case connectionHandler of
            WithInitiatorMode outboundHandler ->
              ConnectionManager
                (WithInitiatorMode
                  OutboundConnectionManager {
                      ocmRequestConnection =
                        requestOutboundConnectionImpl stateVar outboundHandler,
                      ocmUnregisterConnection =
                        unregisterOutboundConnectionImpl stateVar
                    })

            WithResponderMode inboundHandler ->
              ConnectionManager
                (WithResponderMode
                  InboundConnectionManager {
                      icmIncludeConnection =
                        includeInboundConnectionImpl stateVar inboundHandler,
                      icmUnregisterConnection =
                        unregisterInboundConnectionImpl stateVar,
                      icmPromotedToWarmRemote =
                        promotedToWarmRemoteImpl stateVar,
                      icmDemotedToColdRemote =
                        demotedToColdRemoteImpl stateVar,
                      icmNumberOfConnections =
                        readTMVar stateVar >>= countConnections
                    })

            WithInitiatorResponderMode outboundHandler inboundHandler ->
              ConnectionManager
                (WithInitiatorResponderMode
                  OutboundConnectionManager {
                      ocmRequestConnection =
                        requestOutboundConnectionImpl stateVar outboundHandler,
                      ocmUnregisterConnection =
                        unregisterOutboundConnectionImpl stateVar
                    }
                  InboundConnectionManager {
                      icmIncludeConnection =
                        includeInboundConnectionImpl stateVar inboundHandler,
                      icmUnregisterConnection =
                        unregisterInboundConnectionImpl stateVar,
                      icmPromotedToWarmRemote =
                        promotedToWarmRemoteImpl stateVar,
                      icmDemotedToColdRemote =
                        demotedToColdRemoteImpl stateVar,
                      icmNumberOfConnections =
                        readTMVar stateVar >>= countConnections
                    })

    k connectionManager
      `finally` do
        traceWith tracer TrShutdown
        state <- atomically $ readTMVar stateVar
        traverse_
          (\connVar -> do
            -- cleanup handler for that thread will close socket associated
            -- with the thread.  We put each connection in 'TerminatedState' to
            -- guarantee, that non of the connection threads will enter
            -- 'TerminatingState' (and thus delay shutdown for 'tcp_WAIT_TIME'
            -- seconds) when receiving the 'AsyncCancelled' exception.
            connState <- atomically $ do
              connState <- readTVar connVar
              writeTVar connVar (TerminatedState Nothing)
              return connState
            traverse_ cancel (getConnThread connState) )
          state
  where
    countConnections :: ConnectionManagerState peerAddr handle handleError version m
                     -> STM m Int
    countConnections state =
        Map.size
      . Map.filter
              (\connState -> case connState of
                ReservedOutboundState          -> False
                UnnegotiatedState Inbound  _ _ -> True
                UnnegotiatedState Outbound _ _ -> False
                InboundIdleState {}            -> True
                InboundState {}                -> True
                OutboundUniState {}            -> False
                OutboundDupState {}            -> True
                DuplexState {}                 -> True
                TerminatingState {}            -> False
                TerminatedState {}             -> False)
     <$> traverse readTVar state


    -- Start connection thread and run connection handler on it.
    --
    -- TODO: We don't have 'MonadFix' instance for 'IOSim', so we cannot
    -- directly pass 'connVar' (which requires @Async ()@ returned by this
    -- function.  If we had 'MonadFix' at hand We could then also elegantly
    -- eliminate 'PromiseWriter'.
    runConnectionHandler :: StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
                         -> ConnectionHandlerFn handlerTrace peerAddr handle handleError version m
                         -> socket
                         -> peerAddr
                         -> PromiseWriter m (Either handleError (handle, version))
                         -> m (ConnectionId peerAddr, Async m ())
    runConnectionHandler stateVar handler socket peerAddr writer = do
      localAddress <- getLocalAddr cmSnocket socket
      let connId = ConnectionId { remoteAddress = peerAddr
                                , localAddress
                                }

      let cleanup :: m ()
          cleanup = do
            traceWith tracer (TrConnectionCleanup connId)
            wConnVar <- atomically $ do
              state' <- readTMVar stateVar
              case Map.lookup peerAddr state' of
                Nothing      -> return Nowhere
                Just connVar -> do
                  connState <- readTVar connVar
                  case connState of
                    ReservedOutboundState -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ There ()
                    UnnegotiatedState {} -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ There ()
                    OutboundUniState {} -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ There ()
                    OutboundDupState {} -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ There ()
                    InboundIdleState {} -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ Here connVar
                    InboundState {} -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ There ()
                    DuplexState {} -> do
                      writeTVar connVar (TerminatedState Nothing)
                      return $ There ()
                    TerminatingState {} -> do
                      return $ Here connVar
                    TerminatedState {} ->
                      return $ There ()

            case wConnVar of
              Nowhere -> do
                close cmSnocket socket
                traceWith tracer (TrConnectionTerminated connId NotFound Nothing)
              Here connVar -> do
                close cmSnocket socket
                traceWith tracer (TrConnectionTimeWait connId)
                threadDelay cmTimeWaitTimeout
                mInState <- atomically $ do
                  mConnState <- readTMVar stateVar >>= traverse readTVar . Map.lookup peerAddr
                  -- We can always write to `connVar`, a new connection will
                  -- use a new 'TVar', but we can only delete it from
                  -- 'ConnectionManagerState' if it is in 'TerminatingState'.
                  -- It might happen that in 'TerminatingState' the server will
                  -- accept a connection.  It will have a new 'TVar' and insert
                  -- it in the same spot in 'ConnectionManagerState'.
                  writeTVar connVar (TerminatedState Nothing)
                  let mInState = summariseState <$> mConnState
                  case mConnState of
                    Just TerminatingState {} ->
                      modifyTMVarPure_ stateVar (Map.delete peerAddr) $> mInState
                    Just TerminatedState {} ->
                      modifyTMVarPure_ stateVar (Map.delete peerAddr) $> mInState
                    _ ->
                      return mInState
                traceWith tracer (TrConnectionTerminated connId WaitTime mInState)
              There _ -> do
                close cmSnocket socket
                modifyTMVar_ stateVar (pure . Map.delete peerAddr)
                traceWith tracer (TrConnectionTerminated connId Reset Nothing)

      case
        handler
          writer
          (TrConnectionHandler connId `contramap` tracer)
          connId
          (\bearerTimeout ->
            toBearer
              cmSnocket
              bearerTimeout
              (WithMuxBearer connId `contramap` muxTracer)
              socket) of
        Action action errorHandler -> do
          -- start connection thread
          connThread <-
            mask $ \unmask ->
              async $ do
                labelThisThread "conn-handler"
                errorHandler (unmask action `finally` cleanup)
          return ( connId
                 , connThread
                 )


    includeInboundConnectionImpl
        :: HasCallStack
        => StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
        -> ConnectionHandlerFn handlerTrace peerAddr handle handleError version m
        -> socket
        -- ^ resource to include in the state
        -> peerAddr
        -- ^ remote address used as an identifier of the resource
        -> m (Connected peerAddr handle handleError)
    includeInboundConnectionImpl stateVar
                                 handler
                                 socket
                                 peerAddr = do
        let provenance = Inbound
        traceWith tracer (TrIncludeConnection provenance peerAddr)
        (connVar, connId, connThread, reader)
          <- modifyTMVar stateVar $ \state -> do
              (reader, writer) <- newEmptyPromiseIO
              (connId, connThread)
                <- runConnectionHandler stateVar handler
                                        socket peerAddr writer
              traceWith tracer (TrIncludedConnection provenance connId)

              -- Either 
              -- @
              --   Accepted    : ● → UnnegotiatedState Inbound
              --   Overwritten : ● → UnnegotiatedState Inbound
              -- @
              --
              -- This is subtle part, which needs to handle a near simultaneous
              -- open.  We cannot relay on 'ReservedOutboundState' state as
              -- a lock.  It may happen that the `requestOutboundConnection`
              -- will put 'ReservedOutboundState', but before it will call `connect`
              -- the `accept` call will return.  We overwrite the state and
              -- replace the connection state 'TVar' with a fresh one.  Nothing
              -- is blocked on the replaced 'TVar'.
              connVar <-
                atomically $
                  newTVar (UnnegotiatedState provenance connId connThread)
                  >>= \v -> labelTVar v ("conn-state-" ++ show connId) $> v
              return ( Map.insert peerAddr connVar state
                     , (connVar, connId, connThread, reader)
                     )

        res <- atomically $ readPromise reader
        case res of
          Left handleError -> do
            atomically $ do
              writeTVar connVar $
                case classifyHandleError handleError of
                  HandshakeFailure           -> TerminatingState connId connThread
                                                                 (Just handleError)
                  HandshakeProtocolViolation -> TerminatedState  (Just handleError)
              modifyTMVarPure_ stateVar (Map.delete peerAddr)
            return (Disconnected connId (Just handleError))

          Right (handle, version) -> do
            let dataFlow = connectionDataFlow version
            -- TODO: tracing!
            atomically $ do
              connState <- readTVar connVar
              case connState of
                -- Inbound connections cannot be found in this state at this
                -- stage.
                ReservedOutboundState ->
                  throwSTM (withCallStack (ImpossibleState peerAddr))

                -- It is impossible to find a connection in 'OutboundUniState'
                -- or 'OutboundDupState', since 'includeInboundConnection'
                -- blocks until 'InboundState'.  This guarantees that this
                -- transactions runs first in case of race between
                -- 'requestOutboundConnection' and 'includeInboundConnection'.
                OutboundUniState _connId _connThread _handle ->
                  throwSTM (withCallStack (ImpossibleState peerAddr))
                OutboundDupState _connId _connThread _handle _expired ->
                  throwSTM (withCallStack (ImpossibleState peerAddr))

                InboundIdleState {} ->
                  throwSTM (withCallStack (ImpossibleState peerAddr))

                -- At this stage the inbound connection cannot be in
                -- 'InboundState', it would mean that there was another thread
                -- that included that connection, but this would violate @TCP@
                -- constraints.
                InboundState {} ->
                  throwSTM (withCallStack (ImpossibleState peerAddr))

                DuplexState {} ->
                  throwSTM (withCallStack (ImpossibleState peerAddr))

                --
                -- The common case.
                --
                -- Note: we don't set an explicit timeout here.  The
                -- server will set a timeout and call
                -- 'unregisterInboundConnection' when it expires.
                --

                UnnegotiatedState {} ->
                  writeTVar connVar (InboundIdleState connId connThread handle
                                    (connectionDataFlow version))

                TerminatingState {} ->
                  writeTVar connVar (InboundIdleState connId connThread handle
                                    (connectionDataFlow version))

                TerminatedState {} ->
                  writeTVar connVar (InboundIdleState connId connThread handle
                                    (connectionDataFlow version))

            -- Note that we don't set a timeout thread here which would perform
            -- @
            --   Commit^{dataFlow}
            --     : InboundIdleState dataFlow
            --     → TerminatingState
            -- @
            -- This is not needed!  When we return from this call, the inbound
            -- protocol governor will monitor the connection.  Once it becomes
            -- idle, it will call 'unregisterInboundConnection' which will
            -- perform the aforementioned @Commit@ transition.

            traceWith tracer (TrNegotiatedConnection provenance connId dataFlow)
            return (Connected connId dataFlow handle)


    unregisterInboundConnectionImpl
        :: StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
        -> peerAddr
        -> m (OperationResult DemotedToColdRemoteTr)
    unregisterInboundConnectionImpl stateVar peerAddr = do
      traceWith tracer (TrUnregisterConnection Inbound peerAddr)
      (mbThread, result) <- atomically $ do
        state <- readTMVar stateVar
        case Map.lookup peerAddr state of
          Nothing ->
            -- Note: this can happen if the inbound connection manager is
            -- notified late about the connection which has already terminated
            -- at this point.
            pure ( Nothing
                 , UnsupportedState UnknownConnection )
          Just connVar -> do
            connState <- readTVar connVar
            case connState of
              -- In any of the following two states unregistering is not
              -- supported.  'includeInboundConnection' is a synchronous
              -- operation which returns only once the connection is
              -- negotiated.
              ReservedOutboundState ->
                return ( Nothing
                       , UnsupportedState InReservedOutboundState )
              UnnegotiatedState {} ->
                return ( Nothing
                       , UnsupportedState InUnnegotiatedState )

              -- @
              --   TimeoutExpired : OutboundState^\tau Duplex
              --                  → OutboundState      Duplex
              -- @
              OutboundDupState connId connThread handle Ticking -> do
                writeTVar connVar (OutboundDupState connId connThread handle Expired)
                return ( Nothing
                       , OperationSuccess KeepTr )
              OutboundDupState _connId _connThread _handle Expired ->
                assert False $
                return ( Nothing
                       , OperationSuccess KeepTr )

              OutboundUniState _connId _connThread _handle ->
                return ( Nothing
                       , UnsupportedState InOutboundUniState )

              -- @
              --   Commit^{dataFlow} : InboundIdleState dataFlow
              --                     → TerminatingState
              -- @
              --
              -- Note: the 'TrDemotedToColdRemote' is logged by the server.
              InboundIdleState connId connThread _handle _dataFlow -> do
                writeTVar connVar (TerminatingState connId connThread Nothing)
                return ( Just connThread
                       , OperationSuccess CommitTr )

              -- the inbound protocol governor was supposed to call
              -- 'demotedToColdRemote' first.
              InboundState connId connThread _handle dataFlow ->
                assert False $ do
                writeTVar connVar (TerminatingState connId connThread Nothing)
                return ( Just connThread
                       , UnsupportedState (InInboundState dataFlow) )

              -- the inbound connection governor ought to call
              -- 'demotedToColdRemote' first.
              DuplexState connId connThread handle ->
                assert False $ do
                writeTVar connVar (OutboundDupState connId connThread handle Ticking)
                return ( Nothing
                       , UnsupportedState InDuplexState )

              -- If 'unregisterOutboundConnection' is called just before
              -- 'unregisterInboundConnection', the latter one might observe
              -- 'TerminatingState'.
              TerminatingState _connId _connThread _handleError ->
                return ( Nothing
                       , OperationSuccess CommitTr )
              -- However, 'TerminatedState' should not be observable by
              -- 'unregisterInboundConnection', unless 'cmTimeWaitTimeout' is
              -- close to 'serverProtocolIdleTimeout'.
              TerminatedState _handleError ->
                return ( Nothing
                       , UnsupportedState InTerminatedState )

      traverse_ cancel mbThread
      return result


    requestOutboundConnectionImpl
        :: HasCallStack
        => StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
        -> ConnectionHandlerFn handlerTrace peerAddr handle handleError version m
        -> peerAddr
        -> m (Connected peerAddr handle handleError)
    requestOutboundConnectionImpl stateVar handler peerAddr = do
        let provenance = Outbound
        traceWith tracer (TrIncludeConnection provenance peerAddr)
        (tr, connVar, eHandleWedge) <- atomically $ do
          state <- readTMVar stateVar
          case Map.lookup peerAddr state of
            Just connVar -> do
              connState <- readTVar connVar
              let inState = summariseState connState
              case connState of
                ReservedOutboundState ->
                  return ( Just (TrConnectionExists provenance peerAddr inState)
                         , connVar
                         , Left (withCallStack
                                  (ConnectionExists provenance peerAddr))
                         )

                UnnegotiatedState Outbound _connId _connThread -> do
                  return ( Just (TrConnectionExists provenance peerAddr inState)
                         , connVar
                         , Left (withCallStack
                                  (ConnectionExists provenance peerAddr))
                         )

                OutboundUniState {} -> do
                  return ( Just (TrConnectionExists provenance peerAddr inState)
                         , connVar
                         , Left (withCallStack
                                  (ConnectionExists provenance peerAddr))
                         )

                OutboundDupState {} -> do
                  return ( Just (TrConnectionExists provenance peerAddr inState)
                         , connVar
                         , Left (withCallStack
                                  (ConnectionExists provenance peerAddr))
                         )
                InboundIdleState connId connThread handle dataFlow@Duplex -> do
                  -- @
                  --   Awake^{Duplex}_{Local} : InboundIdleState Duplex
                  --                          → OutboundState^\tau Duplex
                  -- @
                  writeTVar connVar (OutboundDupState connId connThread handle Ticking)
                  return ( Just (TrReusedIdleConnection connId)
                         , connVar
                         , Right (Here (Connected connId dataFlow handle))
                         )

                InboundIdleState connId _connThread _handle Unidirectional -> do
                  return ( Just (TrForbiddenConnection connId)
                         , connVar
                         , Left (withCallStack
                                  (ForbiddenConnection connId))
                         )

                UnnegotiatedState Inbound connId _connThread ->
                  -- we must not block inside @modifyTVar stateVar@, we
                  -- return 'There' to indicate that we need to block on
                  -- the connection state.
                  return ( Nothing
                         , connVar
                         , Right (There connId)
                         )

                InboundState connId _connThread _handle Unidirectional -> do
                  -- the remote side negotiated unidirectional connection, we
                  -- cannot re-use it.
                  return ( Just (TrForbiddenConnection connId)
                         , connVar
                         , Left (withCallStack
                                  (ForbiddenConnection connId))
                         )

                InboundState connId connThread handle dataFlow@Duplex -> do
                  -- @
                  --   PromotedToWarm^{Duplex}_{Local} : InboundState Duplex
                  --                                   → DuplexState
                  -- @
                  writeTVar connVar (DuplexState connId connThread handle)
                  return ( Just (TrReusedConnection connId)
                         , connVar
                         , Right (Here (Connected connId dataFlow handle))
                         )

                DuplexState _connId _connThread  _handle ->
                  return ( Just (TrConnectionExists provenance peerAddr inState)
                         , connVar
                         , Left (withCallStack
                                  (ConnectionExists provenance peerAddr))
                         )

                TerminatingState _connId _connThread _handleError ->
                  -- await for 'TerminatedState' or for removal of the
                  -- connection from the state.
                  retry

                TerminatedState _handleError -> do
                  -- the connection terminated; we can reset 'connVar' and
                  -- start afresh.
                  writeTVar connVar ReservedOutboundState
                  return ( Nothing
                         , connVar
                         , Right Nowhere 
                         )

            Nothing -> do
              connVar <- newTVar ReservedOutboundState
              -- record the @connVar@ in 'ConnectionManagerState' we can use
              -- 'swapTMVar' as we did not use 'takeTMVar' at the beginning of
              -- this transaction.  Since we already 'readTMVar', it will not
              -- block.
              _ <- swapTMVar stateVar
                    (Map.insert peerAddr connVar state)
              return ( Nothing
                     , connVar
                     , Right Nowhere
                     )

        traverse_ (traceWith tracer) tr
        case eHandleWedge of
          Left e ->
            throwIO e

          -- connection manager does not have a connection with @peerAddr@.
          Right Nowhere ->
            bracketOnError
              (openToConnect cmSnocket peerAddr)
              (\socket -> do
                  close cmSnocket socket
                  atomically $ do
                    writeTVar connVar (TerminatedState Nothing)
                    modifyTMVarPure_ stateVar (Map.delete peerAddr)
              )
              $ \socket -> do
                (reader, writer) <- newEmptyPromiseIO
                traceWith tracer (TrConnectionNotFound provenance peerAddr)
                addr <-
                  case cmAddressType peerAddr of
                    Nothing -> pure Nothing
                    Just IPv4Address ->
                         traverse_ (bind cmSnocket socket)
                                   cmIPv4Address
                      $> cmIPv4Address
                    Just IPv6Address ->
                         traverse_ (bind cmSnocket socket)
                                   cmIPv6Address
                      $> cmIPv6Address

                --
                -- connect
                --

                traceWith tracer (TrConnect addr peerAddr)
                connect cmSnocket socket peerAddr
                  `catch` \e -> do
                    traceWith tracer (TrConnectError addr peerAddr e)
                    traceDebugState tracer stateVar
                    -- the handler attached by `bracketOnError` will
                    -- reset the state
                    throwIO e

                (connId, connThread)
                  <- runConnectionHandler stateVar handler
                                          socket peerAddr writer
                traceWith tracer (TrIncludedConnection provenance connId)
                res <- atomically (readPromise reader)
                case res of
                  Left handleError -> do
                    modifyTMVar stateVar $ \state -> do
                      -- 'handleError' might be either a handshake negotiation
                      -- a protocol failure (an IO exception, a timeout or
                      -- codec failure).  In the first case we should not reset
                      -- the connection as this is not a protocol error.
                      atomically $ writeTVar connVar $
                        case classifyHandleError handleError of
                          HandshakeFailure ->
                            TerminatingState connId connThread
                                            (Just handleError)
                          HandshakeProtocolViolation ->
                            TerminatedState (Just handleError)
                      return ( Map.delete peerAddr state
                             , Disconnected connId (Just handleError)
                             )

                  -- @
                  --  Connected : ReservedOutboundState
                  --            → UnnegotiatedState Outbound
                  -- @
                  Right (handle, version) -> do
                    let dataFlow = connectionDataFlow version
                    -- We can safely overwrite the state: after successful
                    -- `connect` it's not possible to have a race condition
                    -- with any other inbound thread.  We are also guaranteed
                    -- to have exclusive access as an outbound thread.
                    atomically $
                      writeTVar
                        connVar $
                          case dataFlow of
                            Unidirectional ->
                              OutboundUniState connId connThread handle 
                            Duplex ->
                              OutboundDupState connId connThread handle Ticking
                    traceWith
                      tracer
                      (TrNegotiatedConnection provenance connId dataFlow)
                    return (Connected connId dataFlow handle)

          Right (There connId) -> do
            -- We can only enter the 'There' case if there is an inbound
            -- connection, and we are about to reuse it, but we need to wait
            -- for handshake.
            (tr', connected) <- atomically $ do
              connState <- readTVar connVar
              case connState of
                ReservedOutboundState {} ->
                  throwSTM
                    (withCallStack (ImpossibleState (remoteAddress connId)))
                UnnegotiatedState Outbound _ _ ->
                  throwSTM
                    (withCallStack (ConnectionExists provenance connId))

                UnnegotiatedState Inbound _ _ ->
                  -- await for connection negotiation
                  retry

                OutboundUniState {} ->
                  throwSTM (withCallStack (ConnectionExists provenance connId))

                OutboundDupState {} ->
                  throwSTM (withCallStack (ConnectionExists provenance connId))

                InboundIdleState _connId connThread handle dataFlow@Duplex -> do
                  -- @
                  --   Awake^{Duplex}_{Local} : InboundIdleState Duplex
                  --                          → OutboundState^\tau Duplex
                  -- @
                  -- This transition can happen if there are concurrent
                  -- `includeInboudConnection` and `requestOutboundConnection`
                  -- calls.
                  writeTVar connVar (OutboundDupState connId connThread handle Ticking)
                  return ( TrReusedIdleConnection connId
                         , Connected connId dataFlow handle
                         )

                InboundIdleState _connId _connThread _handle Unidirectional ->
                  throwSTM
                    (withCallStack (ImpossibleState (remoteAddress connId)))

                InboundState _ _ _ Unidirectional ->
                  throwSTM (withCallStack (ForbiddenConnection connId))

                InboundState _connId connThread handle dataFlow@Duplex -> do
                  -- @
                  --   PromotedToWarm^{Duplex}_{Local} : InboundState Duplex
                  --                                   → DuplexState
                  -- @
                  writeTVar connVar (DuplexState connId connThread handle)
                  return ( TrReusedConnection connId
                         , Connected connId dataFlow handle
                         )
                DuplexState {} ->
                  throwSTM (withCallStack (ConnectionExists provenance connId))

                TerminatingState _connId _connThread handleError ->
                  return ( TrTerminatingConnection provenance connId
                         , Disconnected connId handleError
                         )
                TerminatedState handleError ->
                  return ( TrTerminatedConnection provenance
                                                 (remoteAddress connId)
                         , Disconnected connId handleError
                         )

            traceWith tracer tr'
            return connected

          -- Connection manager has a connection which can be reused.
          Right (Here connected) ->
            return connected


    unregisterOutboundConnectionImpl
        :: StrictTMVar m
            (ConnectionManagerState peerAddr handle handleError version m)
        -> peerAddr
        -> m (OperationResult InState)
    unregisterOutboundConnectionImpl stateVar peerAddr = do
      traceWith tracer (TrUnregisterConnection Outbound peerAddr)
      (transition :: DemoteToColdLocal peerAddr handlerTrace
                                       handle handleError version m)
        <- atomically $ do
        state <- readTMVar stateVar
        case Map.lookup peerAddr state of
          -- if the connection errored, it will remove itself from the state.
          -- Calling 'unregisterOutboundConnection' is a no-op in this case.
          Nothing -> pure (DemoteToColdLocalNoop UnknownConnection)

          Just connVar -> do
            connState <- readTVar connVar
            case connState of
              -- In any of the following three states unregistering is not
              -- supported.  'requestOutboundConnection' is a synchronous
              -- operation which returns only once the connection is
              -- negotiated.
              ReservedOutboundState ->
                let inState = InReservedOutboundState in
                return $
                  DemoteToColdLocalError
                    (TrForbiddenOperation peerAddr inState)
                    inState

              UnnegotiatedState {} ->
                let inState = InUnnegotiatedState in
                return $
                  DemoteToColdLocalError
                    (TrForbiddenOperation peerAddr inState)
                    inState

              OutboundUniState connId connThread _handle -> do
                -- @
                --   DemotedToCold^{Unidirectional}_{Local}
                --     : OutboundState Unidirectional
                --     → TerminatingState
                -- @
                writeTVar connVar (TerminatingState connId connThread Nothing)
                return (DemotedToColdLocal connId connThread InOutboundUniState)

              OutboundDupState connId connThread _handle expired@Expired -> do
                -- @
                --   DemotedToCold^{Duplex}_{Local}
                --     : OutboundState^\tau Duplex
                --     → InboundIdleState^\tau
                -- @
                writeTVar connVar (TerminatingState connId connThread Nothing)
                return (DemotedToColdLocal connId connThread
                         (InOutboundDupState expired))

              OutboundDupState connId connThread handle expired@Ticking -> do
                -- @
                --   DemotedToCold^{Duplex}_{Local}
                --     : OutboundState Duplex
                --     → InboundIdleState^\tau Duplex
                -- @
                writeTVar connVar (InboundIdleState connId connThread handle Duplex)
                return (DemoteToColdLocalNoop (InOutboundDupState expired))

              InboundIdleState _connId _connThread _handle dataFlow ->
                assert (dataFlow == Duplex) $
                return (DemoteToColdLocalNoop (InInboundIdleState dataFlow))
              InboundState _peerAddr _connThread _handle dataFlow ->
                assert (dataFlow == Duplex) $ do
                let inState = InInboundState dataFlow
                return $
                  DemoteToColdLocalError
                    (TrForbiddenOperation peerAddr inState)
                    inState

              DuplexState connId connThread handle -> do
                -- @
                --   DemotedToCold^{Duplex}_{Local} : DuplexState
                --                                  → InboundState Duplex
                -- @
                --
                writeTVar connVar (InboundState connId connThread handle Duplex)

                numberOfConns <- countConnections state
                let numberToPrune =
                        numberOfConns
                      - fromIntegral
                          (acceptedConnectionsHardLimit cmConnectionsLimits)
                if numberToPrune > 0
                then do
                  -- traverse the state and get only the connection which
                  -- have 'ConnectionType' and are running (have a thread).
                  -- This excludes connections in 'ReservedOutboundState',
                  -- 'TerminatingState' and 'TerminatedState'.
                  (choiseMap :: Map peerAddr (ConnectionType, Async m ()))
                    <- flip Map.traverseMaybeWithKey state $ \_peerAddr connVar' ->
                         (\connState' ->
                            -- this expression returns @Maybe (connType, connThread)@;
                            -- 'traverseMaybeWithKey' collects all 'Just' cases.
                            (,) <$> getConnType connState' <*> getConnThread connState')
                     <$> readTVar connVar'

                  pruneSet <-
                    cmPrunePolicy
                      (fst <$> choiseMap)
                      numberToPrune
                  return $
                    PruneConnections connId
                      (snd <$> choiseMap `Map.restrictKeys` pruneSet)
                else
                  -- @
                  -- DemotedToCold^{Duplex}_{Local} : DuplexState
                  --                                → InboundState Duplex
                  -- @
                  -- does not require from us to perform any other io action.
                  return (DemoteToColdLocalNoop InDuplexState)

              TerminatingState _connId _connThread _handleError ->
                return (DemoteToColdLocalNoop InTerminatingState)
              TerminatedState _handleError ->
                return (DemoteToColdLocalNoop InTerminatedState)

      case transition of
        DemotedToColdLocal connId connThread inState -> do
          traceWith tracer (TrDemotedToColdLocal connId inState)
          cancel connThread
          -- We relay on the `fianlly` handler of connection thread to:
          --
          -- * close the socket,
          -- * set the state to 'TerminatedState'
          return (OperationSuccess inState)

        PruneConnections connId pruneMap -> do
          traceWith tracer (TrDemotedToColdLocal connId (InInboundState Duplex))
          traceWith tracer (TrPruneConnections (Map.keys pruneMap))
          -- previous comment applies here as well.
          traverse_ cancel pruneMap
          return (OperationSuccess InDuplexState)

        DemoteToColdLocalError tr inState -> do
          traceWith tracer tr
          return (UnsupportedState inState)

        DemoteToColdLocalNoop inState ->
          return (OperationSuccess inState)


    promotedToWarmRemoteImpl
        :: StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
        -> peerAddr
        -> m (OperationResult InState)
    promotedToWarmRemoteImpl stateVar peerAddr = do
      traceDebugState tracer stateVar
      atomically $ do
        mbConnVar <- Map.lookup peerAddr <$> readTMVar stateVar
        case mbConnVar of
          Nothing -> return (UnsupportedState UnknownConnection)
          Just connVar -> do
            connState <- readTVar connVar
            case connState of
              ReservedOutboundState {} ->
                assert False $
                return (UnsupportedState InReservedOutboundState)
              UnnegotiatedState {} ->
                assert False $
                return (UnsupportedState InUnnegotiatedState)
              OutboundUniState _connId _connThread _handle ->
                assert False $
                return (UnsupportedState InOutboundUniState)
              OutboundDupState connId connThread handle _expired -> do
                -- @
                --   PromotedToWarm^{Duplex}_{Remote} : OutboundState Duplex
                --                                    → DuplexState
                -- @
                writeTVar connVar (DuplexState connId connThread handle)
                return (OperationSuccess InDuplexState)
              InboundIdleState connId connThread handle dataFlow -> do
                -- @
                --   Awake^{dataFlow}_{Remote} : InboundIdleState Duplex
                --                             → InboundState Duplex
                -- @
                writeTVar connVar (InboundState connId connThread handle dataFlow)
                return (OperationSuccess (InInboundState dataFlow))
              InboundState _connId _connThread _handle dataFlow ->
                -- already in 'InboundState'?
                assert False $
                return (OperationSuccess (InInboundState dataFlow))
              DuplexState {} ->
                return (OperationSuccess InDuplexState)
              TerminatingState {} ->
                return (UnsupportedState InTerminatingState)
              TerminatedState {} ->
                return (UnsupportedState InTerminatedState)


    demotedToColdRemoteImpl
        :: StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
        -> peerAddr
        -> m (OperationResult InState)
    demotedToColdRemoteImpl stateVar peerAddr =
      atomically $ do
        mbConnVar <- Map.lookup peerAddr <$> readTMVar stateVar
        case mbConnVar of
          Nothing -> return (UnsupportedState UnknownConnection)
          Just connVar -> do
            connState <- readTVar connVar
            case connState of
              ReservedOutboundState {} ->
                assert False $
                return (UnsupportedState InReservedOutboundState)
              UnnegotiatedState {} ->
                assert False $
                return (UnsupportedState InUnnegotiatedState)
              OutboundUniState _connId _connThread _handle ->
                assert False $
                return (UnsupportedState InOutboundUniState)
              OutboundDupState _connId _connThread _handle expired ->
                assert False $
                return (UnsupportedState (InOutboundDupState expired))
              InboundIdleState _connId _connThread _handle dataFlow ->
                assert False $
                return (UnsupportedState (InInboundIdleState dataFlow))

              -- @
              --   DemotedToCold^{dataFlow}_{Remote}
              --     : InboundState dataFlow
              --     → InboundIdleState^\tau dataFlow
              -- @
              InboundState connId connThread handle dataFlow -> do
                writeTVar connVar (InboundIdleState connId connThread handle dataFlow)
                return (OperationSuccess (InInboundState dataFlow))

              -- @
              --   DemotedToCold^{dataFlow}_{Remote}
              --     : DuplexState
              --     → OutboundState^\tau Duplex
              -- @
              DuplexState connId connThread handle -> do
                writeTVar connVar (OutboundDupState connId connThread handle Ticking)
                return (OperationSuccess InDuplexState)

              TerminatingState {} ->
                return (UnsupportedState InTerminatingState)
              TerminatedState {} ->
                return (UnsupportedState InTerminatedState)


--
-- Utilities
--

-- | Like 'modifyMVar_' but strict
--
modifyTMVar_ :: ( MonadSTM  m
                , MonadMask m
                )
             => StrictTMVar m a -> (a -> m a) -> m ()
modifyTMVar_ v io =
    mask $ \unmask -> do
      a <- atomically (takeTMVar v)
      a' <- unmask (io a) `onException` atomically (putTMVar v a)
      atomically (putTMVar v a')


-- | Like 'modifyMVar' but strict in @a@ and for 'TMVar's
--
modifyTMVar :: ( MonadEvaluate m
               , MonadMask     m
               , MonadSTM      m
               )
            => StrictTMVar m a
            -> (a -> m (a, b))
            -> m b
modifyTMVar v k =
  mask $ \restore -> do
    a      <- atomically (takeTMVar v)
    (!a',b) <- restore (k a >>= evaluate) `onException` atomically (putTMVar v a)
    atomically (putTMVar v a')
    return b


-- | Like 'modifyMVar_' but pure.
--
modifyTMVarPure_ :: MonadSTM m
                 => StrictTMVar m a
                 -> (a -> a)
                 -> STM m ()
modifyTMVarPure_ v k = takeTMVar v >>= putTMVar v . k


--
-- Exceptions
--

-- | Useful to attach 'CallStack' to 'ConnectionManagerError'.
--
withCallStack :: HasCallStack => (CallStack -> a) -> a
withCallStack k = k callStack


traceDebugState :: MonadSTM m
                => Tracer m (ConnectionManagerTrace peerAddr handlerTrace)
                -> StrictTMVar m (ConnectionManagerState peerAddr handle handleError version m)
                -> m ()
traceDebugState tracer stateVar = do
    debugState <- atomically $
          readTMVar stateVar
      >>= traverse (fmap summariseState . readTVar)
    traceWith tracer (TrDebugState debugState)
