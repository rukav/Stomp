{-# LANGUAGE ScopedTypeVariables #-}
import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.HUnit

import Network.Stomp
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Control.Exception as E
import Control.Concurrent
import Data.IORef
import Data.Maybe
import Control.Monad
import System.Timeout

main = defaultMain tests
tests = [ 
          testGroup "Stomp commands"
            [ testCase "connect" testConnect
            , testCase "stomp" testStomp
            , testCase "disconnect" testDisconnect
            , testCase "send" testSend
            , testCase "send'" testBatchSend
            , testCase "subscribe" testSubscribe
            , testCase "unsubscribe" testUnsubscribe
            , testCase "ack" testAck
            , testCase "nack" testNack
            , testCase "commit" testCommit
            , testCase "abort" testAbort
            ],
          testGroup "Stomp protocol"
            [ testCase "receipt" testReceipt
            , testCase "sendbeat" testSendbeat
            , testCase "recvbeat" testRecvbeat
            , testCase "beat" testBeat
            , testCase "errors" testErrors
            , testCase "encode" testEncode
            ],
          testGroup "Stomp messages"
            [ testCase "null" testNull
            , testCase "length" testLength
            , testCase "big" testBig
            ]
        ]

testConnect = do
  (con, resp, _) <- mkStomp
  E.finally
    (do r <- readChan' resp
        assertBool "Unexpected stomp response" (isNothing r))
    (disconnect con [])

testStomp = do
  con <- connect uri [] []
  disconnect con []
  unless (maximum (versions con) == (1,0)) $ do
     con' <- stomp uri []
     disconnect con' []

testDisconnect = do
  (con, resp, _) <- mkStomp
  disconnect con []
  E.catch 
    (do disconnect con []
        assertBool "Unexpected valid connection" False)
    (\(e::StompException) -> 
      assertBool "Invalid exception" (isConError e))
    
testSend = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do send con dest [] BL.empty
        r <- readChan' resp
        assertBool "Unexpected send response" (isNothing r)

        let headers = [("receipt","1"), 
                       ("content-type","text/plain"), 
                       ("content-length", show (BL.length testMsg))
                      ]
        send con dest headers testMsg
        Just (Frame (SC RECEIPT) hs _) <- readChan' resp
        let receipt = fromJust $ lookup "receipt-id" hs 
        assertEqual "Receipt for send command" receipt "1")
    (disconnect con [])

testBatchSend = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid []
        send' con $ replicate 50 (dest, [], testMsg)
        forM_ [1..50] $ \_ -> do
            r <- readChan' resp
            assertEqual "Invalid message received" (body (fromJust r)) testMsg)
    (disconnect con [])

testSubscribe = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally 
    (do send con dest [] testMsg
        subscribe con dest subid []
        Just (Frame (SC cmd) hs body) <- readChan' resp
        unsubscribe con subid []
        assertEqual "Wrong broker response" cmd MESSAGE
        assertEqual "Invalid message received" body testMsg
        let sid = fromJust $ lookup "subscription" hs
        assertEqual "Invalid subscription id" sid subid)
    (disconnect con [])

testUnsubscribe = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally 
    (do subscribe con dest subid []
        send con dest [] testMsg
        (Just (Frame (SC cmd) _ _)) <- readChan' resp
        assertEqual "Wrong broker response" cmd MESSAGE
        unsubscribe con subid []
        r <- readChan' resp
        assertBool "Unexpected unsubscribe response" (isNothing r)
        send con dest [] testMsg
        r1 <- readChan' resp
        assertBool "Unexpected response after send" (isNothing r1))
    (disconnect con [])

testAck = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid [("ack", "client")]
        send con dest [] testMsg
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        assertEqual "Invalid message received" body testMsg)
    (disconnect con [])

  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid [("ack", "client")]
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        assertEqual "Invalid message received" body testMsg
        let msgid = fromJust $ lookup "message-id" hs
        ack con subid msgid [])
    (disconnect con [])

  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid [("ack", "client")]
        r <- readChan' resp
        assertBool "Unexpected response after ack" (isNothing r))
    (disconnect con [])

testNack = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (unless (maximum (versions con) == (1,0)) $ do 
        subscribe con dest subid [("ack", "client-individual")]
        send con dest [] testMsg
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        let msgid = fromJust $ lookup "message-id" hs
        nack con subid msgid []
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        assertEqual "Invalid message received" body testMsg
        let msgid = fromJust $ lookup "message-id" hs
        ack con subid msgid [])
    (disconnect con [])
  
testCommit = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid []
        begin con tid []
        send con dest [("transaction",tid)] testMsg
        r <- readChan' resp
        assertBool "Unexpected response before commit" (isNothing r)
        commit con tid []
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        assertEqual "Invalid message received" body testMsg)
    (disconnect con [])

testAbort = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid []
        begin con tid []
        send con dest [("transaction",tid)] testMsg
        r <- readChan' resp
        assertBool "Unexpected response before abort" (isNothing r)
        abort con tid[]
        r' <- readChan' resp
        assertBool "Unexpected response after abort" (isNothing r))
    (disconnect con [])

testReceipt = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid [("receipt","1")]
        readChan' resp >>= \f -> assertReceipt f "1"
        begin con tid [("transaction",tid),("receipt","2")]
        readChan' resp >>= \f -> assertReceipt f "2"
        abort con tid [("transaction",tid),("receipt","3")]
        readChan' resp >>= \f -> assertReceipt f "3")
    (do disconnect con [("receipt","4")]
        readChan' resp >>= \f -> assertReceipt f "4")

testSendbeat = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  if maximum (versions con) > (1,0) then do
     threadDelay (sendTimeout con * 5000)
     E.catch 
       (do disconnect con []
           assertBool "Send heartbeat failure" False)
       (\(e::StompException) -> 
           assertBool "Invalid exception" (isConError e))
     (con, resp, _) <- mkStomp
     startSendBeat con
     threadDelay (sendTimeout con * 5000)
     disconnect con []
   else disconnect con []

testRecvbeat = do
  initDest dest subid
  (con, resp, excp) <- mkStomp
  if maximum (versions con) > (1,0) then do
     startRecvBeat con
     threadDelay (recvTimeout con * 5000)
     e <- readChan' excp
     assertBool "Receive heartbeat failure" (isBrokerError e)
   else disconnect con []
  where   
    isBrokerError (Just (BrokerError _)) = True 
    isBrokerError _ = False

testBeat = do
  initDest dest subid
  (con, resp, excp) <- mkStomp
  E.finally
    (when (maximum (versions con) > (1,0)) $ do 
       beat con
       r <- readChan' resp
       assertBool "Unexpected response after beat" (isNothing r))
    (disconnect con [])

testErrors = do
  E.catch 
    (do
      connect "invalid uri" [] []
      assertBool "Connect test failure" False)
    (\(e::StompException) -> 
      case e of
        InvalidUri _ -> return ()
        _ -> assertBool "Invalid exception" False)

  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do send con "/1queue" [] testMsg
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        assertEqual "Invalid error response" cmd ERROR)
    (disconnect con [])

testEncode = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid [("receipt",":\n\\")]
        readChan' resp >>= \f -> assertReceipt f ":\n\\")
    (disconnect con [])

testNull = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally 
    (do
      subscribe con dest subid []
      send con dest [] msg
      (Just (Frame (SC cmd) hs body)) <- readChan' resp
      assertEqual "Invalid message size" (BL.length body) len
      send con dest [] testMsg
      (Just (Frame (SC cmd) hs body')) <- readChan' resp
      assertEqual "Invalid message" body' testMsg)
    (disconnect con [])
  where 
    msg = BL.pack (replicate 1024 'x' ++ "\x00" ++ replicate 1024 'y')
    len = BL.length msg

testLength = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid []
        send con dest [] msg
        (Just (Frame (SC cmd) hs body)) <- readChan' resp
        assertEqual "Invalid message received" body msg)
    (disconnect con [])
  where 
    msg = BL.pack (replicate 1024 'x' ++ replicate 1024 'y' ++ "\x00\x00zz")

testBig = do
  initDest dest subid
  (con, resp, _) <- mkStomp
  E.finally
    (do subscribe con dest subid []
        send con dest [("content-length", show size)] msg
        (Just (Frame (SC cmd) hs body)) <- readChan'' resp
        assertEqual "Invalid message size" (BL.length body) (fromIntegral size))
    (disconnect con [])
  where 
    size = 1024 * 1024 * 8
    msg = BL.pack (replicate size 'x')

-- | Make default stomp connection with communication channels
mkStomp :: IO (Connection, Chan Frame, Chan StompException)
mkStomp = do
   con <- connect uri [(1,0),(1,1)] [("heart-beat","5000,5000")]
   respChan <- newChan
   excpChan <- newChan
   setExcpHandler con (excpFun con excpChan)
   startConsumer con (respFun con respChan)
   return (con, respChan, excpChan)
   where
     excpFun con = writeChan
     respFun con = writeChan

-- | Check receipt frame
assertReceipt :: Maybe Frame -> String -> IO ()
assertReceipt (Just (Frame (SC cmd) hs _)) r = do
   assertEqual "RECEIPT response expected" cmd RECEIPT
   let r' = fromJust $ lookup "receipt-id" hs
   assertEqual "Wrong receipt value" r' r

-- | Check exception as a connection error
isConError :: StompException -> Bool
isConError (ConnectionError _) = True
isConError _ = False

-- | Default test uri
uri :: String
uri = "stomp://guest:guest@127.0.0.1:61613"

-- | Default test queue destination
dest :: String
dest = "/queue/q"

-- | Default test subscription id
subid :: String
subid = "0"

-- | Default test transaction id
tid :: String
tid = "0"

-- | Test message body
testMsg :: BL.ByteString
testMsg = BL.pack "test message"

-- | Read from channel with 2 sec timeout
readChan' :: Chan a -> IO (Maybe a)
readChan' ch = timeout 2000000 (readChan ch)

-- | Read from channel with 15 sec timeout
readChan'' :: Chan a -> IO (Maybe a)
readChan'' ch = timeout 15000000 (readChan ch)

-- | Initialize the destination before test
initDest :: Destination -> Subscription -> IO ()
initDest dest subid = do
    (con, resp, _) <- mkStomp
    subscribe con dest subid []
    _ <- whileJust (readChan' resp)
    disconnect con []

-- | Recursively collect values contained in the Just  
whileJust :: Monad m => m (Maybe a) -> m [a]
whileJust p = do
  x <- p
  case x of
    Nothing -> return mzero
    Just x -> do
      xs <- whileJust p
      return $ return x `mplus` xs
