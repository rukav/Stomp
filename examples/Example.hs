import Network.Stomp
import qualified Data.ByteString.Lazy.Char8 as B

main = do
  -- connect to a stomp broker
  con <- connect "stomp://guest:guest@127.0.0.1:61613" vers headers
  putStrLn $ "Accepted versions: " ++ show (versions con)
  
  -- start consumer and subscribe to the queue
  startConsumer con callback
  subscribe con "/queue/test" "0" []

  -- send the messages to the queue
  send con "/queue/test" [] (B.pack "message1")
  send con "/queue/test" [] (B.pack "message2")

  -- wait
  getLine
  
  -- unsubscribe and disconnect
  unsubscribe con "0" []
  disconnect con []
  where 
    vers = [(1,0),(1,1)]
    headers = []

callback :: Frame -> IO ()
callback (Frame (SC MESSAGE) hs body) = do
      putStrLn $ "received message: " ++ (B.unpack body) 
      putStrLn $ "headers: " ++ show hs
callback f = putStrLn $ "received frame: " ++ show f