pub mod processor;
use std::cell::RefCell;
use std::rc::Rc;
use crate::DBResult;
use crate::blocks::{BlockRef, ColumnBlock};


pub enum ProcessStatus
{
    MustStop,
    MustGoOn
}

pub trait Processor
{
    fn run(&mut self, input :BlockRef, output :BlockRef) -> DBResult<ProcessStatus>;
}

pub trait PostProcessor
{
    fn run(&mut self, output :BlockRef) -> DBResult<()>;
}

pub type ProcessorRef = Rc<RefCell<dyn Processor>>;
pub type PostProcessorRef = Rc<RefCell<dyn PostProcessor>>;

pub struct ExecuteStep
{
    input :BlockRef,
    output :BlockRef,
    processors :Vec<ProcessorRef>,
    post_processors :Vec<PostProcessorRef>,
}

impl ExecuteStep
{
    pub fn new(input: ColumnBlock, output: ColumnBlock) -> Self
    {
        Self {
            input :Rc::new(RefCell::new(input)),
            output :Rc::new(RefCell::new(output)),
            processors: Vec::<ProcessorRef>::new(),
            post_processors: Vec::<PostProcessorRef>::new()
        }
    }

    pub fn add_proc(&mut self, proc:ProcessorRef) -> &mut Self
    {
        self.processors.push(proc);
        self
    }

    pub fn add_post_proc(&mut self, proc:PostProcessorRef) -> &mut Self
    {
        self.post_processors.push(proc);
        self
    }

    pub fn output(&self) -> BlockRef
    {
        self.output.clone()
    }


    pub fn execute(&mut self) -> DBResult<()>
    {
        let mut stopped = false;
        while !stopped {
            for p in self.processors.iter_mut()
            {
                match p.borrow_mut().run(self.input.clone(), self.output.clone())? {
                    ProcessStatus::MustStop => {stopped = true; break},
                    ProcessStatus::MustGoOn => {}
                }
            }
        }
        for p in self.post_processors.iter_mut()
        {
            p.borrow_mut().run(self.output.clone())?;
        }
        Ok(())
    }
}